CREATE OR REPLACE PROCEDURE PROD_DB.PROD_SCHEMA.PY_PROCEDURE(from_table VARCHAR, join_table VARCHAR, to_table VARCHAR)
RETURNS VARCHAR(500)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.8'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'run'
EXECUTE AS OWNER
AS
$$
def run(session, from_table, join_table, to_table):
    truncate_table = session.sql(f"""TRUNCATE TABLE {to_table}""").collect()
    
    cust_table = session.sql(f"""INSERT INTO {to_table}
                            (
                                CUSTOMER_ID,         
                                DATE,                
                                SALUTATION,          
                                FIRST_NAME,          
                                LAST_NAME
                             )
                    SELECT C_CUSTOMER_SK
                                , TO_CHAR(D_DATE, 'YYYYMMDD')
                                , C_SALUTATION
                                , C_FIRST_NAME
                                , C_LAST_NAME
                    FROM {from_table} C
                    JOIN {join_table} D 
                    ON D_DATE_SK = C_FIRST_SHIPTO_DATE_SK
                    LIMIT 100""").collect();
                             
    return cust_table;
$$;
