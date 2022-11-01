from asyncio import exceptions
import pandas as pd

def run(session, from_table, join_table, to_table):
    try:
        cust_table = session.sql(f"""INSERT INTO {to_table}
                                            (
                                                CUSTOMER_ID,         
                                                DATE,                
                                                SALUTATION,          
                                                FIRST_NAME,          
                                                LAST_NAME,
                                                DATE_INSERT
                                            )
                                     SELECT C_CUSTOMER_SK
                                            , TO_CHAR(D_DATE, 'YYYYMMDD')
                                            , C_SALUTATION
                                            , C_FIRST_NAME
                                            , C_LAST_NAME
                                            , CURRENT_TIMESTAMP()
                                            FROM {from_table} C
                                            JOIN {join_table} D 
                                            ON D_DATE_SK = C_FIRST_SHIPTO_DATE_SK
                                            LIMIT 100""").collect()
                        
        cust_table_2 = session.sql(f"""INSERT INTO {to_table}
                                            (
                                                CUSTOMER_ID,         
                                                DATE,                
                                                SALUTATION,          
                                                FIRST_NAME,          
                                                LAST_NAME,
                                                DATE_INSERT
                                            )
                                    SELECT C_CUSTOMER_SK
                                                , TO_CHAR(D_DATE, 'YYYYMMDD')
                                                , C_SALUTATION
                                                , C_FIRST_NAME
                                                , C_LAST_NAME
                                                , CURRENT_TIMESTAMP()
                                    FROM {from_table} C
                                    JOIN {join_table} D 
                                    ON D_DATE_SK = C_FIRST_SHIPTO_DATE_SK
                                    LIMIT 100""").collect()
        
    except exceptions:
        print("Idk some error")
                    
    return i,j