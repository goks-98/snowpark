import os
from snowflake.snowpark import Session
import requests
import pandas as pd
import json

connection_parameters = {
"account": "ld77469.uae-north.azure",
"user": "GOKS98",
"password": "March20311",
"role": "ACCOUNTADMIN",
"warehouse": "SNOWFLAKE_WAREHOUSE",
"database": "OIL_AND_GAS",
"schema": "CONFORMED"
 }

test_session = Session.builder.configs(connection_parameters).create()

response = requests.get("https://api.openweathermap.org/data/2.5/weather?lat=25.2048&lon=55.2708&appid=b3effbd4d36024a4b9735d33830e7ca3")
data = response.json()
df = pd.json_normalize(data)
df.head()

cust_sql = test_session.table("OIL_AND_GAS.CONFORMED.CUSTOMER_TABLE")
cust_sql.show(10)