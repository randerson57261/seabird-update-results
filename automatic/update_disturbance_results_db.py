import pandas as pd
from arcgis.gis import GIS
import numpy as np
import os
from sqlalchemy import create_engine
from dotenv import load_dotenv  
load_dotenv() 

#-----------------------------------------#
#Use this script to automatically pull data from the arcGIS database,
#do the analysis, and update the website's result database
#-----------------------------------------#


pd.options.display.max_columns = None
pd.set_option('display.max_rows', 500)
pd.options.mode.chained_assignment = None

DATABASE_URL = os.environ.get('SEABIRD_DATABASE_URL')
#DATABASE_URL = 'postgresql://ryan:4520@localhost/seabird'

#connect to arcGIS online  
gis = GIS()
print("Logged in as anonymous user to " + gis.properties.portalName)

#get by direct ID
feature_service = gis.content.get('e40fcb8191c1427ab09836f62ccd8340')

#get survey and observations tables feature services
survey_fs = feature_service.layers[0]
events_fs = feature_service.tables[1]
affects_fs = feature_service.tables[2]


#Get data
surveys = survey_fs.query().sdf
events = events_fs.query().sdf
affects = affects_fs.query().sdf


# # QAQC
print('QAQC...')

#Format datetime
surveys['date'] = surveys.start_date.dt.strftime('%Y-%m-%-d')
surveys['date_m_d'] = surveys.start_date.dt.strftime('%m-%-d')
surveys['year'] = surveys.start_date.dt.year
surveys['week'] = surveys.start_date.dt.strftime('%Y-%U')

#Remove surveys that are not approved
#this is done before the next cell so that a preference over the daily duplicate can be selected, overriding the selection of the earliest one.
surveys_a = surveys.loc[surveys.approved == 'a',:]

#Flag daily duplicate surveys
surveys_a['survey_count'] = surveys_a.groupby(['date','group_name','survey_area','count_block']).count_block.transform('count')
surveys_a['min_date'] = surveys_a.groupby(['date','group_name','survey_area','count_block']).start_date.transform(min)
surveys_a.loc[(surveys_a.survey_count > 1) & (surveys_a.min_date != surveys_a.start_date),'QAQC'] = 'Duplicate'

# Flag any visibility 5 and beaufort 8 data (bad weather)
surveys_a.loc[surveys_a.visibility == 5,'QAQC'] = 'Poor Visibility'
surveys_a.loc[surveys_a.beaufort == 8,'QAQC'] = 'Poor Sea State'

# Flag any entires where observer marked "not assessable"
# This also should flag any entry where sp_count == None
surveys_a.loc[surveys_a.assessable == 'n','QAQC'] = 'Not Assessable'

#Subset to only surveys where disturbance was recorded (excludes unknowns) and to only non flagged surveys
surveys_sub = surveys_a.loc[((surveys_a.disturbance_obs == 'no') | (surveys_a.disturbance_obs == 'yes')) & surveys_a.QAQC.isna(),:]


# # Analysis
print('Analysis...')

#Identify unique surveys
surveys_sub['key'] = surveys_sub.group_name + surveys_sub.survey_area + surveys_sub.date
surveys_sub['survey_ID'] = surveys_sub.groupby(['key']).ngroup()

#Survey interval
surveys_sub['survey_start'] = surveys_sub.groupby(['survey_ID']).start_date.transform(min)
surveys_sub['survey_end'] = surveys_sub.groupby(['survey_ID']).end_date.transform(max)

surveys_sub['interval'] = surveys_sub.survey_end - surveys_sub.survey_start

#Merge with disturbance events
merged = surveys_sub.merge(events, left_on='globalid', right_on='parentglobalid', how='left')

#Count number of disturbance events and calculate disturbance rate
event_count = merged.groupby('survey_ID').globalid_y.count()
intervals = merged.groupby('survey_ID').interval.first()

rate = pd.concat([event_count, intervals], axis=1)
rate.columns = ['events','interval']

rate['interval_days'] = rate['interval'].dt.total_seconds()/86400
rate['disturb_rate'] = rate.events / rate.interval_days

#Merge disturbance rate with survey
rate_atr = rate.merge(surveys_sub, on='survey_ID')

#Find mean disturbance rate
rate_atr['year'] = rate_atr.start_date.dt.year
results = rate_atr.groupby(['group_name','survey_area','year']).disturb_rate.mean().reset_index()

results = results.rename(columns={'disturb_rate':'disturbs_per_day'})

print(results)
results.to_csv("disturbance.csv")
#update internal results database
alchemyEngine = create_engine(DATABASE_URL)

# Connect to PostgreSQL server
dbConnection = alchemyEngine.connect()

#Send dataframe to database
try:
    results.to_sql('Disturb_Results', alchemyEngine, if_exists='replace')
except ValueError as vx:
    print(vx)
except Exception as ex:
    print(ex)
else:
    print("PostgreSQL Table Species has been created successfully.")