CREATE OR REPLACE PROCEDURE TEMP_DB.PUBLIC.JS_PROCEDURE()
RETURNS VARCHAR(500)
LANGUAGE JAVASCRIPT
EXECUTE AS OWNER
AS
$$
    var result = 'Complete!'
    
	my_statement = snowflake.createStatement( {sqlText: `INSERT INTO "TEMP_DB"."PUBLIC"."CUSTOMER_TABLE"
                                                            (CUSTOMER_ID
                                                            ,DATE
                                                            ,SALUTATION
                                                            ,FIRST_NAME
                                                            ,LAST_NAME)
                                                        SELECT C_CUSTOMER_SK
                                                            ,TO_CHAR(D_DATE,'YYYYMMDD')
                                                            ,C_SALUTATION
                                                            ,C_FIRST_NAME
                                                            ,C_LAST_NAME 
                                                        FROM SNOWFLAKE_SAMPLE_DATA.TPCDS_SF100TCL.CUSTOMER C 
                                                        JOIN SNOWFLAKE_SAMPLE_DATA.TPCDS_SF100TCL.DATE_DIM D 
                                                        ON D_DATE_SK = C_FIRST_SHIPTO_DATE_SK 
                                                        LIMIT 100`
                                                });
										
	try {
			my_statement.execute()	
		}
		
	catch(err) {
				result = "Failed:\nCode: " +err.code+ "\n State: " +err.state;
				result += "\nMessage: " +err.message;
				result += "\nStack Trace:\n" +err.stackTracetxt;
				}
	return result;

$$;