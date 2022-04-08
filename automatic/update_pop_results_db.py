import pandas as pd
from arcgis.gis import GIS
import numpy as np
import os
from sqlalchemy import create_engine
from dotenv import load_dotenv  
load_dotenv()                    

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

#get survey and observations tables
observation_fs = feature_service.tables[0] 
survey_fs = feature_service.layers[0]

#Get features
surveys = survey_fs.query().sdf

#Get observations
obs = observation_fs.query().sdf


# # Flag Data

print('QAQC...')


#Remove columns
obs = obs.drop(columns=['survey_ID'])
surveys = surveys.drop(columns=['survey_ID'])


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


#Merge - drop rows where survey is nan (non existant or not approved)
data = surveys_a.merge(obs, left_on='globalid', right_on='parentglobalid', how='outer')
data = data.loc[data.globalid_x.notna(),:]

# Flag any visibility 5 and beaufort 8 data (bad weather)
data.loc[data.visibility == 5,'QAQC'] = 'Poor Visibility'
data.loc[data.beaufort == 8,'QAQC'] = 'Poor Sea State'

# Flag any entires where observer marked "not assessable"
# This also should flag any entry where sp_count == None
data.loc[data.assessable == 'n','QAQC'] = 'Not Assessable'

#Flag surveys with <60% of blocks counted
# count of blocks for each area, add parameters as new sites are added
shell_beach = 7
remainder = 10

data['cblock_count'] = data.groupby(['QAQC','date','group_name','survey_area']).count_block.transform('nunique')
data.loc[(data.survey_area == 'shell_beach') &(data.cblock_count < shell_beach * .6) & (data.QAQC.isna()),'QAQC'] = '<60% of blocks counted'
data.loc[(data.survey_area != 'shell_beach') &(data.cblock_count < remainder * .6) & (data.QAQC.isna()),'QAQC'] = '<60% of blocks counted'


#Subset to only data that is not flagged
g_data = data.loc[data.QAQC.isna(),:]


# # Inspect Flagged Data

#flagged_data = data.loc[data.QAQC.notna(),:]
#flagged_data.to_excel('SeabirdAware_Flagged_Data_20210127.xlsx')


# # Breeding Population -  Pigeon Guillemont

print('Summarizing Pigeon Guillemont...')


#Subset to before June 15th, and only PIGU species
pigu = g_data.loc[(g_data.date_m_d < '06-15') & (g_data.species == 'pigu'),:]


#Create area sum column, the sum of counts for the area that the record is a part of
pigu['area_sum'] = pigu.groupby(['year','group_name','survey_area','date']).sp_count.transform(sum)

#Create a season max column, the max count for the area the record is a part of
pigu['season_max'] = pigu.groupby(['year','group_name','survey_area']).area_sum.transform(max)


#subset to records where area_sum is same as season_max
pigu_results = pigu.loc[pigu.area_sum == pigu.season_max,:]

#Reformatting to results table
pigu_results = pigu_results.loc[:,['group_name','survey_area','count_block','year','species','sp_count']]
pigu_results = pigu_results.reset_index()
pigu_final = pigu_results.rename(columns={'sp_count':'population'})
pigu_final = pigu_final.drop(columns='index')

# # Breeding Population - BRAC, PECO, WEGU, BLOY, DCCO

print('Summarizing Other Species...')

other = g_data.loc[g_data.species.isin(['brac','peco','wegu','bloy','dcco']) ,:]


#Average surveys conducted in the same week
other_m = other.groupby(['group_name','survey_area','count_block','year','week','species']).nest_count.mean()
other_m = other_m.drop(columns=['week']).reset_index()

ranks = other_m.groupby(['year','group_name','survey_area','count_block','species']).nest_count.rank(ascending=False, method='first')
ranks.name = 'rank'
other_r = pd.concat([other_m, ranks], axis = 1)

#other_r.loc[(other_r.year==2019) & (other_r.group_name=='mcas') & (other_r.survey_area=='shell_beach') &(other_r.species=='wegu') & (other_r.count_block==3),:]


#Sort data, subet to only 3rd rank data, 2x nest count to get population
other_results = other_r.sort_values(['group_name','survey_area','count_block','year','species'])
other_results['population'] = other_results.nest_count * 2
other_final = other_results.loc[other_results['rank'] == 3,['group_name','survey_area','count_block','year','species','population']]


# # Roosting/Rafting/Hauled Out - All species
print('Calculating Rooting/Rafting/Hauled out for all species...')

#Mean of multiple surveys per week
rrh_results = g_data.groupby(['group_name','survey_area','count_block','year','week','species']).sp_count.mean()

#Mean population at count block for the year
rrh_results = rrh_results.groupby(['group_name','survey_area','count_block','year','species']).mean().reset_index()

#Rename column
rrh_final = rrh_results.rename(columns={'sp_count':'population'})

#Round population up
rrh_final['rrh'] = rrh_final.population.apply(np.ceil)


# # Vandenberg Analysis

# # Load Trinidad 2014-2018 results


#trinidad_14_18 = pd.read_csv("trinidad_results_14_18_v2021_0114")

#trinidad_14_18.to_pickle('trinidad_results_14_18_v2021_0114')


trinidad_14_18 = pd.read_pickle('trinidad_results_14_18_v2021_0114')


# # Combine results
#Breeding
b_results = pd.concat([other_final,pigu_final,trinidad_14_18 ], axis=0)

#Roosting/rafting/hauled out
rrh_results = pd.concat([rrh_final,trinidad_14_18], axis=0)
rrh_results = rrh_results.drop(columns='population')

#results.to_excel('SeabirdAware_Summarized_20210127.xlsx')
#b_results.to_csv("b_population_2022_0327.csv")
#rrh_results.to_csv("rrh_population_2022_0327.csv")

print("Sending to DB")
# Internal database
alchemyEngine = create_engine(DATABASE_URL)


# Connect to PostgreSQL server
dbConnection = alchemyEngine.connect()

#Send dataframe to database
try:
    b_results.to_sql('b_results', alchemyEngine, if_exists='replace')
except ValueError as vx:
    print(vx)
except Exception as ex:
    print(ex)
else:
    print("PostgreSQL B Table Species has been created successfully.")
    
#Send dataframe to database
try:
    rrh_results.to_sql('rrh_results', alchemyEngine, if_exists='replace')
except ValueError as vx:
    print(vx)
except Exception as ex:
    print(ex)
else:
    print("PostgreSQL RRH Table Species has been created successfully.")