import pandas as pd
import os
from sqlalchemy import create_engine
from dotenv import load_dotenv  


# Use this script to update the website's distrurbance 
# result database using a csv file.

#CD to \Update-Results\manual and run from there

#---------Load Databse URL from .env file ---------------#
load_dotenv() #You need to have the .env file located in \manual
#This is the databse URL

#--------------Open CSV file --------------------------------#
results = pd.read_csv("data/disturbance_2022_0403.csv")

#--------------update internal results database ---------------#
DATABASE_URL = os.environ.get('SEABIRD_DATABASE_URL')
#DATABASE_URL = 'postgresql://ryan:4520@localhost/seabird'

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
    print("PostgreSQL table Disturb_Results has been created successfully.")