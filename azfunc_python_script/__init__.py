#Azure Func IMplementation
import logging
from snowflake.snowpark import Session
import requests
import pandas as pd
import azure.functions as func

def main(req: func.HttpRequest) -> func.HttpResponse:
    
    #Connecting to snowflake
    connection_parameters = {
    "account": "yw79382.uae-north.azure",
    "user": "GOKUL98",
    "password": "M@rch20311",
    "role": "ACCOUNTADMIN",
    "warehouse": "COMPUTE_WH",
    "database": "TEMP_DB",
    "schema": "PUBLIC"
    }

    session = Session.builder.configs(connection_parameters).create()
    
    logging.info('Python HTTP trigger function processed a request.')
    #Creating a Dataframe
    df_table = session.table("SNOWFLAKE_SAMPLE_DATA.TPCDS_SF10TCL.WAREHOUSE")

    #Converting to pandas df to make manipulation easier
    df_pd = df_table.to_pandas()

    #Getting weather data from API for every zipcode
    json_list = []
    try:
        for i in df_pd['W_ZIP']:
            response = requests.get(f"http://api.openweathermap.org/geo/1.0/zip?zip={i},US&appid=b597b3c1633fe8722c905f4d1a97a4b5")
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

    return func.HttpResponse("Success !", status_code=200)
    
