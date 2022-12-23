USE DATABASE {{target_db}};
USE SCHEMA {{target_schema}};
-----------------------------------

CREATE TABLE HELLO_WORLD
(
   FIRST_NAME VARCHAR
  ,LAST_NAME VARCHAR
);
-----------------------------------

CREATE TABLE DIM_DATE 
AS
SELECT * FROM SNOWFLAKE_SAMPLE_DATA.TPCDS_SF100TCL.DATE_DIM;
-----------------------------------------------------------
