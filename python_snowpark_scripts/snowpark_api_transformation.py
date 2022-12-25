#Sample snowpark use case that connects to an api to do simple transformations on a snowflake sample table 
#and load into temp_db database

#Importing required libraries
from snowflake.snowpark import Session
import requests
import pandas as pd
import os
from dotenv import load_dotenv

#load environment variables
load_dotenv()

api_key = os.getenv("api_key")
account = os.getenv("account")
user = os.getenv("user")
password = os.getenv("password")
role = os.getenv("role")
warehouse = os.getenv("warehouse")
database = os.getenv("database")
schema = os.getenv("schema")

#Connecting to snowflake
connection_parameters = {
"account": account,
"user": user,
"password": password,
"role": role,
"warehouse": warehouse,
"database": database,
"schema": schema
}

#Creating a snowflake session
session = Session.builder.configs(connection_parameters).create()

#Creating a snowflake dataframe
df_table = session.table("SNOWFLAKE_SAMPLE_DATA.TPCDS_SF10TCL.WAREHOUSE")

#Converting it to pandas df to make manipulation easier
df_pd = df_table.to_pandas()

#Getting weather data from API for every zipcode
json_list = []
try:
    for i in df_pd['W_ZIP']:
        response = requests.get(f"http://api.openweathermap.org/geo/1.0/zip?zip={i},US&appid={api_key}")
        data = response.json()
        json_list.append(data)
except ValueError:
    pass

#Creating a new df with weather info and zip code
zip_code = df_pd['W_ZIP']
data = list(zip(zip_code, json_list))
weather_df = pd.DataFrame(data=data, columns=['W_ZIP', 'WEATHER_JSON'])

#Joining weather nad warehouse tables
snowflake_df = df_pd.merge(weather_df, how='inner', on='W_ZIP')

#Writing table to snowflake
snowflake_df_1 = session.create_dataframe(snowflake_df)
snowflake_df_1.write.mode("overwrite").save_as_table("TEMP_DB.PUBLIC.warehouse_weather_api")

#End of Code


    
