TRUNCATE TABLE CUSTOMER_TABLE;

CREATE OR REPLACE PROCEDURE TEMP_DB.PUBLIC.SQL_PROCEDURE()
RETURNS VARCHAR(50)
LANGUAGE SQL
EXECUTE AS OWNER
AS
declare res resultset;
begin
   res:= (INSERT INTO TEMP_DB.PUBLIC.CUSTOMER_TABLE
        (CUSTOMER_ID, DATE, SALUTATION, FIRST_NAME, LAST_NAME)
        SELECT C_CUSTOMER_SK
               ,TO_CHAR(D_DATE,'YYYYMMDD')
               ,C_SALUTATION
               ,C_FIRST_NAME
               ,C_LAST_NAME 
        FROM SNOWFLAKE_SAMPLE_DATA.TPCDS_SF100TCL.CUSTOMER C 
        JOIN SNOWFLAKE_SAMPLE_DATA.TPCDS_SF100TCL.DATE_DIM D 
        ON D_DATE_SK = C_FIRST_SHIPTO_DATE_SK 
        LIMIT 100);
end;

call sql_procedure();

SELECT * FROM CUSTOMER_TABLE;




create or replace procedure area()
returns float
language sql
as
$$
declare
    radius float;
    area_of_circle float;
begin
    radius := 3;
    area_of_circle := pi() * radius * radius;
    return area_of_circle;
end;
$$
;


call area();


create or replace procedure count_greater_than(table_name varchar, maximum_count integer)
returns boolean not null
language sql
as
declare
  res1 resultset;
begin
  res1 := (call get_row_count(:table_name));
  let c1 cursor for res1;
  for row_variable in c1 do
    if (row_variable.get_row_count > maximum_count) then
      return true;
    else
      return false;
    end if;
 end for;
end;


create or replace procedure get_row_count(table_name varchar)
returns integer not null
language sql
as
declare
  row_count integer default 0;
  res resultset default (select count(*) as count from identifier(:table_name));
  c1 cursor for res;
begin
  for row_variable in c1 do
    row_count := row_variable.count;
end for;
  return row_count;
end;




CALL count_greater_than('customer_table', 60);























