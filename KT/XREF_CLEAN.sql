CREATE OR REPLACE PROCEDURE DAAS_COMMON.GUEST_CLEAN_XREF_LKP_LOAD_PROC (BATCH_ID FLOAT, SHARD_NAME VARCHAR, WAREHOUSE_NAME VARCHAR, LEVEL VARCHAR) 
RETURNS VARCHAR
LANGUAGE JAVASCRIPT 
EXECUTE AS CALLER AS $$ var proc_output = "";
/* 
#####################################################################################
Author: Yuvarani M
Purpose: Load data from DAAS_RAW_PDB.GST_ID_XREF_RAW to DAAS_CORE.GUEST_XREF_CLEAN_LKP table
Input Parameters: BATCH_ID, SHARD_NAME, WAREHOUSE_NAME, CUSTOM_PARAM1, CUSTOM_PARAM2
Output Value: SUCCESS for successful execution and FAILED for unsuccessful execution 
Create Date: 09/02/2022
Version: 1.0
Author: Hemanth  
Purpose: Implemented -1 logic to clean table  and when there is any correction in the data then -1 value should update with the correct value
Steps added:   1)var my_sql_command_fetch_child_data 
               2)var my_sql_command_update_DEDUP_TABLE 
			   3)var my_sql_command_fetch_child_data_level_100
			   4)my_sql_command_update_child_data_level_100,
			   5)my_sql_command_SRC_GUEST_XREF_CLEAN_FINAL 
			   6)source_table change in merge ( SRC_GUEST_XREF_CLEAN_LKP_REC_CHECK_TEMP to SRC_GUEST_XREF_CLEAN_FINAL_TEMP)   
Version: 1.1
Modified By: Yuvarani M
Modification: 1)removed all columns (i.e., select *) in recursive CTEs and implementing only main columns (i.e., select col1,col2,..)
              2)changed union to union all in subqueries of recursive CTEs
Modified Date: 12/23/2022
Version: 1.2
Modified By: Rohit Kumar Das
Modified Date: 05/10/2022
Modification: 1) Included XREF_TYPE as joining condition for GUEST_XREF_CLEAN_LKP Merge statement
			  2) Handling Infinite records and new parent child relation in the same batch
#####################################################################################
*/
proc_output = "";
proc_step = "";
snowflake.execute( {sqlText: "USE WAREHOUSE " + WAREHOUSE_NAME} );
tag = BATCH_ID + "_GUEST_CLEAN_XREF_LKP_LOAD_PROC";
snowflake.execute( {sqlText: "ALTER SESSION SET QUERY_TAG = '" + tag + "'" });

/* create temp1 table*/
snowflake.execute( {sqlText: `CREATE OR REPLACE TABLE DAAS_TEMP.GUEST_XREF_TMP AS SELECT * FROM DAAS_CORE.GUEST_XREF_LKP LIMIT 0;`} );

/* create temp2 table*/
snowflake.execute( {sqlText: `CREATE OR REPLACE TABLE DAAS_TEMP.GUEST_XREF_TEMP AS SELECT * FROM DAAS_TEMP.GUEST_XREF_TMP LIMIT 0;`} );

snowflake.execute( {sqlText: `CREATE OR REPLACE TABLE DAAS_TEMP.SRC_GUEST_XREF_CLEAN_LKP_DEDUP_TEMP AS SELECT * FROM DAAS_CORE.GUEST_XREF_LKP LIMIT 0;`} );

snowflake.execute( {sqlText: `TRUNCATE TABLE DAAS_TEMP.SRC_GUEST_XREF_CLEAN_LKP_REC_TEMP;`} );


snowflake.execute( {sqlText: `CREATE OR REPLACE TABLE DAAS_TEMP.SRC_GUEST_XREF_CLEAN_INFINITE_CHK_TEMP AS SELECT * FROM DAAS_CORE.GUEST_XREF_CLEAN_LKP LIMIT 0 ;`} );

snowflake.execute( {sqlText: `CREATE OR REPLACE TABLE DAAS_TEMP.SRC_GUEST_XREF_CLEAN_LKP_REC_CHECK_TEMP AS SELECT * FROM DAAS_TEMP.SRC_GUEST_XREF_CLEAN_LKP_REC_TEMP LIMIT 0 ;`} );

snowflake.execute( {sqlText: `CREATE OR REPLACE TABLE DAAS_TEMP.GUEST_XREF_CTE_FROM_RAW_TEMP AS  select * from  DAAS_TEMP.SRC_GUEST_XREF_CLEAN_LKP_REC_TEMP LIMIT 0;`} );

/*snowflake.execute( {sqlText: `CREATE  OR REPLACE  TABLE DAAS_TEMP.SRC_GUEST_XREF_CLEAN_FINAL_TEMP AS SELECT * FROM DAAS_TEMP.SRC_GUEST_XREF_CLEAN_LKP_REC_CHECK_TEMP LIMIT 0;`} );*/

snowflake.execute( {sqlText: `TRUNCATE TABLE DAAS_TEMP.SRC_GUEST_XREF_CLEAN_FINAL_TEMP;`} );


/* raw to temp1 load*/
my_sql_raw_to_temp_load = 
`INSERT INTO DAAS_TEMP.GUEST_XREF_TMP
SELECT 
    PRIMARY_PROP_ID,
	PRIMARY_GUEST_ID,
	XREF_PROP_ID,
	XREF_GUEST_ID,
    PRIMARY_ACCOUNT_NBR,
    XREF_ACCOUNT_NBR,
    XREF_TYPE,
    SOURCE_SYSTEM_NM,
	TIME_ZONE,
    CREATED_DTTM,
    CREATED_BY,
    UPDATED_DTTM,
    UPDATED_BY,
    DELETE_IND,
    BATCH_ID,
    GST_ID_XREF_RAW_BATCH_ID,
    NULL AS LAST_DML_CD,
	KEY_SEQUENCE_NBR
FROM
(	
   SELECT
	RAW.I_PROP_ID AS PRIMARY_PROP_ID,
	RAW.I_GUEST_ID AS PRIMARY_GUEST_ID,
	RAW.I_XREF_PROP_ID AS XREF_PROP_ID,
	RAW.I_XREF_GUEST_ID AS XREF_GUEST_ID,
	(RAW.I_PROP_ID * 100000000 + RAW.I_GUEST_ID) As PRIMARY_ACCOUNT_NBR,
    (RAW.I_XREF_PROP_ID * 100000000 + RAW.I_XREF_GUEST_ID) As XREF_ACCOUNT_NBR,
    RAW.C_XREF_TYPE AS XREF_TYPE,
	RAW.SOURCE_SYSTEM_NAME AS SOURCE_SYSTEM_NM,
	RAW.TIME_ZONE,
    CURRENT_TIMESTAMP() AS CREATED_DTTM,
	CURRENT_USER() AS CREATED_BY,
	CURRENT_TIMESTAMP() AS UPDATED_DTTM,
	CURRENT_USER() AS UPDATED_BY,
    CASE
		WHEN RAW.OPERATION_TYPE = 'D' THEN 'Y'
		ELSE 'N'
	END DELETE_IND,
    ` + BATCH_ID + ` AS BATCH_ID,
	RAW.BATCH_ID AS GST_ID_XREF_RAW_BATCH_ID,
	ROW_NUMBER() OVER (PARTITION BY 
			RAW.I_XREF_PROP_ID,
			RAW.I_XREF_GUEST_ID
			
		ORDER BY
			KEY_SEQUENCE DESC) RANK,
	KEY_SEQUENCE AS KEY_SEQUENCE_NBR
	FROM
		DAAS_RAW_PDB.GUEST_XREF_RAW_STREAM  RAW
	WHERE
		RAW.METADATA$ACTION = 'INSERT'
)
WHERE
	RANK = 1`;

/*intermediate data set to temp 2*/	

my_sql_command_temp_clean_join = 
	`INSERT INTO DAAS_TEMP.GUEST_XREF_TEMP
	SELECT CLN.PRIMARY_PROP_ID,
	CLN.PRIMARY_GUEST_ID,
	CLN.XREF_PROP_ID,
	CLN.XREF_GUEST_ID,
    CLN.PRIMARY_ACCOUNT_NBR,
    CLN.XREF_ACCOUNT_NBR,
    CLN.XREF_TYPE,
    CLN.SOURCE_SYSTEM_NM,
	CLN.TIME_ZONE,
    CLN.CREATED_DTTM,
    CLN.CREATED_BY,
    CLN.UPDATED_DTTM,
    CLN.UPDATED_BY,
    CLN.DELETE_IND,
    CLN.BATCH_ID,
    CLN.GST_ID_XREF_RAW_BATCH_ID,
    NULL AS LAST_DML_CD,
	CLN.KEY_SEQUENCE_NBR FROM DAAS_TEMP.GUEST_XREF_TMP TEMP1 INNER JOIN DAAS_CORE.GUEST_XREF_CLEAN_LKP CLN
ON TEMP1.XREF_ACCOUNT_NBR = CLN.XREF_ACCOUNT_NBR WHERE TEMP1.DELETE_IND ='N'
UNION
SELECT  TEMP1.PRIMARY_PROP_ID,
	TEMP1.PRIMARY_GUEST_ID,
	TEMP1.XREF_PROP_ID,
	TEMP1.XREF_GUEST_ID,
    TEMP1.PRIMARY_ACCOUNT_NBR,
    TEMP1.XREF_ACCOUNT_NBR,
    TEMP1.XREF_TYPE,
    TEMP1.SOURCE_SYSTEM_NM,
	TEMP1.TIME_ZONE,
    TEMP1.CREATED_DTTM,
    TEMP1.CREATED_BY,
    TEMP1.UPDATED_DTTM,
    TEMP1.UPDATED_BY,
    TEMP1.DELETE_IND,
    TEMP1.BATCH_ID,
    TEMP1.GST_ID_XREF_RAW_BATCH_ID,
    NULL AS LAST_DML_CD,
	TEMP1.KEY_SEQUENCE_NBR FROM DAAS_TEMP.GUEST_XREF_TMP TEMP1;`;


/*dedup logic*/

var my_sql_command_dedup_xref = `
INSERT INTO DAAS_TEMP.SRC_GUEST_XREF_CLEAN_LKP_DEDUP_TEMP
WITH 
 CTE_XREF_LKP AS
    (
       SELECT * FROM DAAS_TEMP.GUEST_XREF_TEMP XREF 
	   WHERE XREF.PRIMARY_ACCOUNT_NBR <> XREF.XREF_ACCOUNT_NBR
       QUALIFY ROW_NUMBER() OVER (PARTITION BY XREF.PRIMARY_ACCOUNT_NBR, XREF.XREF_ACCOUNT_NBR 
	   ORDER BY XREF.KEY_SEQUENCE_NBR DESC) =1 
    ),
 CTE_XREF_DUP AS --FIND ALL XREF ACCOUNT NUMBERS, WHERE CHILD IS HAVING MORE THAN ONE PARENTS (DUPLICATES)
    (SELECT XREF_ACCOUNT_NBR, COUNT(DISTINCT PRIMARY_ACCOUNT_NBR)
     FROM CTE_XREF_LKP
     GROUP BY XREF_ACCOUNT_NBR
     HAVING COUNT(DISTINCT PRIMARY_ACCOUNT_NBR) > 1
    ),  
 CTE_XREF_NONDUP AS --FIND ALL XREF ACCOUNT NUMBERS, WHERE CHILD IS HAVING ONLY ONE PARENT (NON DUPLICATE RECORDS)
    (SELECT XREF_ACCOUNT_NBR,COUNT(DISTINCT PRIMARY_ACCOUNT_NBR)
     FROM CTE_XREF_LKP
     GROUP BY XREF_ACCOUNT_NBR
     HAVING COUNT(DISTINCT PRIMARY_ACCOUNT_NBR) = 1
    ), 
  CTE_XREF_DUP_DS AS --FIND ALL DUPLICATE RECORDS WITH ALL COLUMNS
    (SELECT XREF.*
     FROM CTE_XREF_DUP XREF_DUP
     INNER JOIN CTE_XREF_LKP XREF ON XREF_DUP.XREF_ACCOUNT_NBR = XREF.XREF_ACCOUNT_NBR
    ), 
    
   CTE_XREF_NONDUP_DS AS --FIND ALL NON DUPLICATE RECORDS WITH ALL COLUMNS
    (SELECT XREF.* 
	FROM CTE_XREF_NONDUP XREF_NONDUP
     INNER JOIN CTE_XREF_LKP XREF ON XREF_NONDUP.XREF_ACCOUNT_NBR = XREF.XREF_ACCOUNT_NBR
    ), 
  
  CTE_GUEST_DIM AS --FIND ALL ACTIVE SURVIVORS FROM GUEST DIM
    (SELECT * FROM DAAS_CORE.GUEST_DIM GUEST_DIM
     WHERE GUEST_DIM.DELETE_IND = 'N'
	 QUALIFY ROW_NUMBER() OVER (PARTITION BY GUEST_DIM.GUEST_UNIQUE_ID 
	 ORDER BY GUEST_DIM.SOURCE_LAST_CHANGED_DT DESC) =1
     ), 
  XREF_AND_GUEST_LKP_DS AS --LEFT JOIN DUPLICATE CTE DATASET WITH GUEST DIM CTE
    (SELECT XREF_DUP.*,GST_DIM.GUEST_UNIQUE_ID,GST_DIM.SOURCE_LAST_CHANGED_DT
     FROM CTE_XREF_DUP_DS XREF_DUP
     LEFT JOIN CTE_GUEST_DIM GST_DIM ON XREF_DUP.PRIMARY_ACCOUNT_NBR = GST_DIM.GUEST_UNIQUE_ID
    ),
	CTE_GUEST_EXIST AS
	(
	SELECT 
	XREF_GST_LKP.PRIMARY_PROP_ID, 
	XREF_GST_LKP.PRIMARY_GUEST_ID,
	XREF_GST_LKP.XREF_PROP_ID,
	XREF_GST_LKP.XREF_GUEST_ID,
	XREF_GST_LKP.PRIMARY_ACCOUNT_NBR,
	XREF_GST_LKP.XREF_ACCOUNT_NBR,
	XREF_GST_LKP.XREF_TYPE, 
	XREF_GST_LKP.SOURCE_SYSTEM_NM, 
	XREF_GST_LKP.TIME_ZONE, 
	XREF_GST_LKP.CREATED_DTTM, 
	XREF_GST_LKP.CREATED_BY, 
	XREF_GST_LKP.UPDATED_DTTM, 
	XREF_GST_LKP.UPDATED_BY,
	XREF_GST_LKP.DELETE_IND,
	XREF_GST_LKP.BATCH_ID, 
	XREF_GST_LKP.GST_ID_XREF_RAW_BATCH_ID, 
	XREF_GST_LKP.LAST_DML_CD, 
	XREF_GST_LKP.KEY_SEQUENCE_NBR
	FROM XREF_AND_GUEST_LKP_DS XREF_GST_LKP
	WHERE XREF_GST_LKP.GUEST_UNIQUE_ID IS NOT NULL  
	and XREF_GST_LKP.PRIMARY_ACCOUNT_NBR <> -1
    QUALIFY ROW_NUMBER() OVER (PARTITION BY XREF_GST_LKP.XREF_ACCOUNT_NBR
    ORDER BY XREF_GST_LKP.KEY_SEQUENCE_NBR DESC, XREF_GST_LKP.SOURCE_LAST_CHANGED_DT DESC) =1
	),
	CTE_GUEST_NOT_EXIST AS
	(
	SELECT 
	XREF_GST_LKP.PRIMARY_PROP_ID, 
	XREF_GST_LKP.PRIMARY_GUEST_ID, 
	XREF_GST_LKP.XREF_PROP_ID, 
	XREF_GST_LKP.XREF_GUEST_ID,
	XREF_GST_LKP.PRIMARY_ACCOUNT_NBR, 
	XREF_GST_LKP.XREF_ACCOUNT_NBR, 
	XREF_GST_LKP.XREF_TYPE, 
	XREF_GST_LKP.SOURCE_SYSTEM_NM, 
	XREF_GST_LKP.TIME_ZONE, 
	XREF_GST_LKP.CREATED_DTTM, 
	XREF_GST_LKP.CREATED_BY, 
	XREF_GST_LKP.UPDATED_DTTM, 
	XREF_GST_LKP.UPDATED_BY, 
	XREF_GST_LKP.DELETE_IND, 
	XREF_GST_LKP.BATCH_ID, 
	XREF_GST_LKP.GST_ID_XREF_RAW_BATCH_ID, 
	XREF_GST_LKP.LAST_DML_CD, 
	XREF_GST_LKP.KEY_SEQUENCE_NBR
	FROM XREF_AND_GUEST_LKP_DS XREF_GST_LKP
	WHERE XREF_GST_LKP.GUEST_UNIQUE_ID IS NULL 
	AND NOT  EXISTS (SELECT XREF_ACCOUNT_NBR FROM CTE_GUEST_EXIST GUEST_EXIST WHERE XREF_GST_LKP.XREF_ACCOUNT_NBR = GUEST_EXIST.XREF_ACCOUNT_NBR)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY XREF_GST_LKP.XREF_ACCOUNT_NBR ORDER BY XREF_GST_LKP.KEY_SEQUENCE_NBR DESC) =1
	)
SELECT * FROM CTE_GUEST_EXIST
UNION
SELECT * FROM CTE_GUEST_NOT_EXIST
UNION
SELECT * FROM CTE_XREF_NONDUP_DS
`;

/*Find a ultimate survior recursively to handle the infinite loop scenarios*/
var my_sql_command_infinite_loop =`
INSERT INTO DAAS_TEMP.SRC_GUEST_XREF_CLEAN_LKP_REC_TEMP
WITH RECURSIVE GUEST_XREF_CTE
AS 
   (
   SELECT 
   0 AS LEVEL ,
	XREF.XREF_GUEST_ID,
   XREF.PRIMARY_ACCOUNT_NBR,  
   XREF.XREF_ACCOUNT_NBR, 
   XREF.DELETE_IND
   
   FROM  DAAS_TEMP.SRC_GUEST_XREF_CLEAN_LKP_DEDUP_TEMP XREF 
   WHERE  XREF.XREF_ACCOUNT_NBR <>  XREF.PRIMARY_ACCOUNT_NBR 
     
   UNION ALL
   SELECT 
   LEVEL+1 AS LEVEL , 
   GUEST_XREF_CTE.XREF_GUEST_ID,
   XREF.PRIMARY_ACCOUNT_NBR, 
   GUEST_XREF_CTE.XREF_ACCOUNT_NBR, 
   XREF.DELETE_IND
        FROM 
     ( SELECT XREF_GUEST_ID, PRIMARY_ACCOUNT_NBR, XREF_ACCOUNT_NBR, DELETE_IND FROM 
      (SELECT XREF.XREF_GUEST_ID, XREF.PRIMARY_ACCOUNT_NBR, XREF.XREF_ACCOUNT_NBR, XREF.DELETE_IND, XREF.KEY_SEQUENCE_NBR
        FROM DAAS_CORE.GUEST_XREF_CLEAN_LKP XREF WHERE XREF.DELETE_IND = 'N'
	  
      UNION ALL
	  
      SELECT XREF_GUEST_ID, PRIMARY_ACCOUNT_NBR, XREF_ACCOUNT_NBR, DELETE_IND,KEY_SEQUENCE_NBR
       FROM DAAS_TEMP.SRC_GUEST_XREF_CLEAN_LKP_DEDUP_TEMP) A QUALIFY ROW_NUMBER() 
                OVER (PARTITION BY A.XREF_ACCOUNT_NBR ORDER BY KEY_SEQUENCE_NBR DESC) = 1 --To handle scenarios when soft delete and infinte loop is received in the same batch
       )XREF, GUEST_XREF_CTE 
   WHERE GUEST_XREF_CTE.PRIMARY_ACCOUNT_NBR=XREF.XREF_ACCOUNT_NBR 
   AND XREF.PRIMARY_ACCOUNT_NBR <> XREF.XREF_ACCOUNT_NBR 
   AND XREF.DELETE_IND = 'N' 
   AND LEVEL <`+LEVEL+`
   ),
   CTE AS (
   SELECT LEVEL,XREF_GUEST_ID,PRIMARY_ACCOUNT_NBR,XREF_ACCOUNT_NBR,DELETE_IND FROM GUEST_XREF_CTE 
    QUALIFY ROW_NUMBER() OVER (PARTITION BY XREF_ACCOUNT_NBR ORDER BY LEVEL DESC) =1
   )
  SELECT CTE.LEVEL,CTE.XREF_GUEST_ID,CTE.PRIMARY_ACCOUNT_NBR,CTE.XREF_ACCOUNT_NBR,CTE.DELETE_IND
  FROM CTE
  `;
 /* inserting infinite records to one table */
 var my_sql_command_src_count = `SELECT COUNT(*) AS COUNT_LEVEL FROM DAAS_TEMP.SRC_GUEST_XREF_CLEAN_LKP_REC_TEMP WHERE LEVEL =`+LEVEL+``;
insert_into_infinite_table=`
INSERT INTO DAAS_CORE.XREF_ALERT_RECORDS 
	 ( 
	SELECT 
	INFINITE_RECORDS.LEVEL AS LEVEL,
	INFINITE_RECORDS.PRIMARY_ACCOUNT_NBR,
	INFINITE_RECORDS.XREF_ACCOUNT_NBR,
	'PDB' AS SOURCE_SYSTEM_NM,
    'US/Central' AS TIME_ZONE,
    `+ BATCH_ID + ` AS BATCH_ID,
    CURRENT_TIMESTAMP() AS CREATED_DTTM,
    CURRENT_USER() AS CREATED_BY,
    CURRENT_TIMESTAMP() AS UPDATED_DTTM,
    CURRENT_USER() AS UPDATED_BY,
	'INFINITE' AS ALERT_TYPE
	FROM DAAS_TEMP.SRC_GUEST_XREF_CLEAN_LKP_REC_TEMP INFINITE_RECORDS
	WHERE INFINITE_RECORDS.LEVEL =`+LEVEL +`)`;  
	
var my_sql_command_infinite_loop_child =`     
INSERT INTO DAAS_TEMP.SRC_GUEST_XREF_CLEAN_LKP_REC_TEMP
select  
 LEVEL, 
 XREF_GUEST_ID,
 -1 PRIMARY_ACCOUNT_NBR, 
 XREF_ACCOUNT_NBR,  
 DELETE_IND 
from DAAS_TEMP.SRC_GUEST_XREF_CLEAN_LKP_REC_TEMP TEMP
     where exists 
	   (select 1 from DAAS_CORE.GUEST_XREF_CLEAN_LKP CLN where   CLN.XREF_ACCOUNT_NBR=TEMP.XREF_ACCOUNT_NBR and TEMP.PRIMARY_ACCOUNT_NBR=-1)
     and not exists  
	   (select 1 from DAAS_CORE.GUEST_XREF_CLEAN_LKP CLN where  CLN.XREF_ACCOUNT_NBR=TEMP.PRIMARY_ACCOUNT_NBR and TEMP.PRIMARY_ACCOUNT_NBR=-1)
/*UNION 
select  LEVEL, 
 XREF_GUEST_ID,
 -1 PRIMARY_ACCOUNT_NBR, 
 XREF_ACCOUNT_NBR,  
 DELETE_IND from 
DAAS_TEMP.SRC_GUEST_XREF_CLEAN_LKP_REC_TEMP TEMP
WHERE exists ( select 1 from DAAS_TEMP.GUEST_XREF_TEMP temp1 where TEMP.xref_account_nbr= temp1.xref_account_nbr and temp1.primary_account_nbr=-1)*/
`;
     
 
/*Find all childs for xref_account_number which tag to -1 */

var my_sql_command_fetch_child_data=` 
insert into  DAAS_TEMP.GUEST_XREF_CTE_FROM_RAW_TEMP
WITH RECURSIVE GUEST_XREF_CTE_FROM_RAW
AS 
   (
   SELECT 
   0 AS LEVEL , 
   XREF.XREF_GUEST_ID,
   XREF.PRIMARY_ACCOUNT_NBR,  
   XREF.XREF_ACCOUNT_NBR, 
   XREF.DELETE_IND 
   FROM  DAAS_TEMP.SRC_GUEST_XREF_CLEAN_LKP_REC_TEMP XREF WHERE  XREF.XREF_ACCOUNT_NBR <>  XREF.PRIMARY_ACCOUNT_NBR  
   and XREF.PRIMARY_ACCOUNT_NBR = -1
   
   union all  
   
   SELECT 
   LEVEL+1 AS LEVEL,
   XREF.XREF_GUEST_ID,
   XREF.PRIMARY_ACCOUNT_NBR,
   XREF.XREF_ACCOUNT_NBR,
   GUEST_XREF_CTE.DELETE_IND
   FROM 
     ( 
      SELECT 
	  I_XREF_GUEST_ID  AS XREF_GUEST_ID,
	  (XREF_RAW.I_PROP_ID * 100000000 + XREF_RAW.I_GUEST_ID) AS PRIMARY_ACCOUNT_NBR,
	  (XREF_RAW.I_XREF_PROP_ID * 100000000 + XREF_RAW.I_XREF_GUEST_ID) AS XREF_ACCOUNT_NBR,
	  XREF_RAW.DELETE_IND
      FROM DAAS_RAW_PDB.GST_ID_XREF_RAW  XREF_RAW  
      where  I_XREF_GUEST_ID<>I_GUEST_ID
      and  not exists 
	      (select 1 from  DAAS_TEMP.SRC_GUEST_XREF_CLEAN_LKP_REC_TEMP where  XREF_ACCOUNT_NBR=(XREF_RAW.I_XREF_PROP_ID * 100000000 + XREF_RAW.I_XREF_GUEST_ID)  and PRIMARY_ACCOUNT_NBR = -1 )      
     )XREF, GUEST_XREF_CTE_FROM_RAW AS GUEST_XREF_CTE 
   WHERE GUEST_XREF_CTE.XREF_ACCOUNT_NBR =XREF.PRIMARY_ACCOUNT_NBR 
   AND XREF.PRIMARY_ACCOUNT_NBR <> XREF.XREF_ACCOUNT_NBR 
   AND XREF.DELETE_IND = 'N' 
   AND LEVEL < `+LEVEL +`
    ),
   CTE AS (
   SELECT * FROM GUEST_XREF_CTE_FROM_RAW  where   PRIMARY_ACCOUNT_NBR <> -1
    QUALIFY ROW_NUMBER() OVER (PARTITION BY XREF_ACCOUNT_NBR ORDER BY LEVEL) =1
   )
  SELECT CTE.*
  FROM CTE ;`; 

 

/* updating existing record PRIMARY_ACCOUNT_NBR in dedup table to xref_account_number in SRC_GUEST_XREF_CLEAN_LKP_REC_TEMP for -1 records to maintain correct data in  DEDUP TEMP table */
var my_sql_command_update_DEDUP_TABLE=`
     update DAAS_TEMP.SRC_GUEST_XREF_CLEAN_LKP_DEDUP_TEMP DEDUP 
	 set DEDUP.PRIMARY_ACCOUNT_NBR=CLEAN_LKP.XREF_ACCOUNT_NBR ,DEDUP.PRIMARY_GUEST_ID=CLEAN_LKP.XREF_GUEST_ID 
from DAAS_TEMP.SRC_GUEST_XREF_CLEAN_LKP_REC_TEMP  CLEAN_LKP
where CLEAN_LKP.XREF_ACCOUNT_NBR=DEDUP.XREF_ACCOUNT_NBR 
and CLEAN_LKP.PRIMARY_ACCOUNT_NBR = -1 
and exists (select 1 from DAAS_CORE.GUEST_XREF_CLEAN_LKP CLN where   DEDUP.XREF_ACCOUNT_NBR=CLN.XREF_ACCOUNT_NBR)
and exists (select 1 from DAAS_CORE.GUEST_XREF_CLEAN_LKP CLN where   DEDUP.PRIMARY_ACCOUNT_NBR=CLN.XREF_ACCOUNT_NBR)`;

     

   


/* FETCHING CHILD RECORDS for  INFINITE SCENARIOS */

var my_sql_command_fetch_child_data_level_100=`
INSERT INTO DAAS_TEMP.SRC_GUEST_XREF_CLEAN_LKP_REC_CHECK_TEMP
WITH RECURSIVE GUEST_XREF_CTE
AS 
   (
   SELECT 
   0 AS LEVEL ,
   XREF.XREF_GUEST_ID,
   XREF.PRIMARY_ACCOUNT_NBR,  
   XREF.XREF_ACCOUNT_NBR,
   XREF.DELETE_IND
     FROM  DAAS_TEMP.SRC_GUEST_XREF_CLEAN_LKP_REC_TEMP XREF 
	 WHERE  XREF.XREF_ACCOUNT_NBR <>  XREF.PRIMARY_ACCOUNT_NBR 
	 and XREF.LEVEL =`+LEVEL +`
     
   UNION ALL
   
   SELECT 
   LEVEL+1 AS LEVEL ,
   XREF.XREF_GUEST_ID,
   XREF.PRIMARY_ACCOUNT_NBR,  
   XREF.XREF_ACCOUNT_NBR,
   XREF.DELETE_IND
     FROM 
     (
      SELECT  
	  XREF.XREF_GUEST_ID,
	  XREF.PRIMARY_ACCOUNT_NBR,  
	  XREF.XREF_ACCOUNT_NBR,
	  XREF.DELETE_IND
	  FROM DAAS_CORE.GUEST_XREF_CLEAN_LKP XREF WHERE XREF.DELETE_IND = 'N'  
	  
      UNION ALL
	  
      SELECT 
	    DEDUP_TEMP.XREF_GUEST_ID,
		DEDUP_TEMP.PRIMARY_ACCOUNT_NBR,  
		DEDUP_TEMP.XREF_ACCOUNT_NBR,
		DEDUP_TEMP.DELETE_IND
	FROM DAAS_TEMP.SRC_GUEST_XREF_CLEAN_LKP_DEDUP_TEMP DEDUP_TEMP
        )XREF, GUEST_XREF_CTE 
   WHERE GUEST_XREF_CTE.PRIMARY_ACCOUNT_NBR=XREF.XREF_ACCOUNT_NBR 
   AND XREF.PRIMARY_ACCOUNT_NBR <> XREF.XREF_ACCOUNT_NBR 
   AND XREF.DELETE_IND = 'N' 
   AND LEVEL < `+LEVEL +`
   ),
   CTE AS (
   SELECT * FROM GUEST_XREF_CTE 
    QUALIFY ROW_NUMBER() OVER (PARTITION BY XREF_ACCOUNT_NBR ORDER BY LEVEL) =1
   )
  SELECT CTE.*
  FROM CTE 
 `;

/* PREPARING FINAL TABLE */

var my_sql_command_update_child_data_level_100 =`
UPDATE DAAS_TEMP.SRC_GUEST_XREF_CLEAN_LKP_REC_CHECK_TEMP
set PRIMARY_ACCOUNT_NBR =-1 `;
 
var my_sql_command_SRC_GUEST_XREF_CLEAN_FINAL =`
INSERT INTO DAAS_TEMP.SRC_GUEST_XREF_CLEAN_FINAL_TEMP
select 
    CHECK_TEMP.LEVEL,
    COALESCE (CLEAN.PRIMARY_PROP_ID,DEDUP_TEMP.PRIMARY_PROP_ID)AS PRIMARY_PROP_ID ,
	COALESCE (CLEAN.PRIMARY_GUEST_ID,DEDUP_TEMP.PRIMARY_GUEST_ID) AS PRIMARY_GUEST_ID,
	COALESCE (CLEAN.XREF_PROP_ID,DEDUP_TEMP.XREF_PROP_ID) AS XREF_PROP_ID,
	CHECK_TEMP.XREF_GUEST_ID,
    CHECK_TEMP.PRIMARY_ACCOUNT_NBR,
    CHECK_TEMP.XREF_ACCOUNT_NBR,
    COALESCE (CLEAN.XREF_TYPE,DEDUP_TEMP.XREF_TYPE) AS XREF_TYPE,
    COALESCE(CLEAN.SOURCE_SYSTEM_NM,DEDUP_TEMP.SOURCE_SYSTEM_NM) AS SOURCE_SYSTEM_NM,
	COALESCE(CLEAN.TIME_ZONE,DEDUP_TEMP.TIME_ZONE) AS TIME_ZONE,
    COALESCE(CLEAN.CREATED_DTTM,DEDUP_TEMP.CREATED_DTTM) AS CREATED_DTTM,
    COALESCE(CLEAN.CREATED_BY,DEDUP_TEMP.CREATED_BY) AS CREATED_BY,
    COALESCE(CLEAN.UPDATED_DTTM,DEDUP_TEMP.UPDATED_DTTM) AS UPDATED_DTTM,
    COALESCE(CLEAN.UPDATED_BY,DEDUP_TEMP.UPDATED_BY)AS UPDATED_BY,
    CHECK_TEMP.DELETE_IND,
    `+BATCH_ID+` AS BATCH_ID,
	COALESCE(CLEAN.GST_ID_XREF_RAW_BATCH_ID,DEDUP_TEMP.GST_ID_XREF_RAW_BATCH_ID) AS GST_ID_XREF_RAW_BATCH_ID,
	CLEAN.LAST_DML_CD,
	COALESCE(CLEAN.KEY_SEQUENCE_NBR, DEDUP_TEMP.KEY_SEQUENCE_NBR) AS KEY_SEQUENCE_NBR
from DAAS_TEMP.SRC_GUEST_XREF_CLEAN_LKP_REC_CHECK_TEMP CHECK_TEMP
LEFT JOIN DAAS_CORE.GUEST_XREF_CLEAN_LKP CLEAN
ON CHECK_TEMP.XREF_ACCOUNT_NBR = CLEAN.XREF_ACCOUNT_NBR  
AND CLEAN.DELETE_IND='N'
LEFT JOIN DAAS_TEMP.SRC_GUEST_XREF_CLEAN_LKP_DEDUP_TEMP DEDUP_TEMP
ON CHECK_TEMP.XREF_ACCOUNT_NBR = DEDUP_TEMP.XREF_ACCOUNT_NBR 
--inner join with dedup
union
select 
REC_TEMP.LEVEL,
    DEDUP_TEMP.PRIMARY_PROP_ID,
	DEDUP_TEMP.PRIMARY_GUEST_ID,
	DEDUP_TEMP.XREF_PROP_ID,
	DEDUP_TEMP.XREF_GUEST_ID,
    DEDUP_TEMP.PRIMARY_ACCOUNT_NBR,
    DEDUP_TEMP.XREF_ACCOUNT_NBR,
    DEDUP_TEMP.XREF_TYPE,
    DEDUP_TEMP.SOURCE_SYSTEM_NM,
	DEDUP_TEMP.TIME_ZONE,
    DEDUP_TEMP.CREATED_DTTM,
    DEDUP_TEMP.CREATED_BY,
    DEDUP_TEMP.UPDATED_DTTM,
    DEDUP_TEMP.UPDATED_BY,
    DEDUP_TEMP.DELETE_IND,
    `+BATCH_ID+` AS BATCH_ID,
	DEDUP_TEMP.GST_ID_XREF_RAW_BATCH_ID,
	DEDUP_TEMP.LAST_DML_CD,
	DEDUP_TEMP.KEY_SEQUENCE_NBR 
from DAAS_TEMP.SRC_GUEST_XREF_CLEAN_LKP_DEDUP_TEMP DEDUP_TEMP 
INNER JOIN DAAS_TEMP.SRC_GUEST_XREF_CLEAN_LKP_REC_TEMP REC_TEMP 
ON DEDUP_TEMP.XREF_ACCOUNT_NBR = REC_TEMP.XREF_ACCOUNT_NBR 
AND  REC_TEMP.LEVEL < `+LEVEL +` 
union  
select 
    RAW_TEMP.LEVEL,
    COALESCE (CLEAN.PRIMARY_PROP_ID,DEDUP_TEMP.PRIMARY_PROP_ID)AS PRIMARY_PROP_ID ,
	COALESCE (CLEAN.PRIMARY_GUEST_ID,DEDUP_TEMP.PRIMARY_GUEST_ID) AS PRIMARY_GUEST_ID,
	COALESCE (CLEAN.XREF_PROP_ID,DEDUP_TEMP.XREF_PROP_ID) AS XREF_PROP_ID,
	RAW_TEMP.XREF_GUEST_ID,
    RAW_TEMP.PRIMARY_ACCOUNT_NBR,
    RAW_TEMP.XREF_ACCOUNT_NBR,
    COALESCE (CLEAN.XREF_TYPE,DEDUP_TEMP.XREF_TYPE) AS XREF_TYPE,
    COALESCE(CLEAN.SOURCE_SYSTEM_NM,DEDUP_TEMP.SOURCE_SYSTEM_NM) AS SOURCE_SYSTEM_NM,
	COALESCE(CLEAN.TIME_ZONE,DEDUP_TEMP.TIME_ZONE) AS TIME_ZONE,
    COALESCE(CLEAN.CREATED_DTTM,DEDUP_TEMP.CREATED_DTTM) AS CREATED_DTTM,
    COALESCE(CLEAN.CREATED_BY,DEDUP_TEMP.CREATED_BY) AS CREATED_BY,
    COALESCE(CLEAN.UPDATED_DTTM,DEDUP_TEMP.UPDATED_DTTM) AS UPDATED_DTTM,
    COALESCE(CLEAN.UPDATED_BY,DEDUP_TEMP.UPDATED_BY)AS UPDATED_BY,
    RAW_TEMP.DELETE_IND,
    `+BATCH_ID+` AS BATCH_ID,
	COALESCE(CLEAN.GST_ID_XREF_RAW_BATCH_ID,DEDUP_TEMP.GST_ID_XREF_RAW_BATCH_ID) AS GST_ID_XREF_RAW_BATCH_ID,
	CLEAN.LAST_DML_CD,
	COALESCE(CLEAN.KEY_SEQUENCE_NBR, DEDUP_TEMP.KEY_SEQUENCE_NBR) AS KEY_SEQUENCE_NBR
	from DAAS_TEMP.GUEST_XREF_CTE_FROM_RAW_TEMP RAW_TEMP
/*INNER JOIN DAAS_TEMP.SRC_GUEST_XREF_CLEAN_LKP_DEDUP_TEMP DEDUP_TEMP
ON RAW_TEMP.XREF_ACCOUNT_NBR = DEDUP_TEMP.XREF_ACCOUNT_NBR */
LEFT JOIN DAAS_CORE.GUEST_XREF_CLEAN_LKP CLEAN
ON RAW_TEMP.XREF_ACCOUNT_NBR = CLEAN.XREF_ACCOUNT_NBR  
AND CLEAN.DELETE_IND='N'
LEFT JOIN DAAS_TEMP.SRC_GUEST_XREF_CLEAN_LKP_DEDUP_TEMP DEDUP_TEMP
ON RAW_TEMP.XREF_ACCOUNT_NBR = DEDUP_TEMP.XREF_ACCOUNT_NBR  ;
`; 

my_sql_command_temp_to_core_load = `
MERGE INTO DAAS_CORE.GUEST_XREF_CLEAN_LKP AS DIM
USING 
	DAAS_TEMP.SRC_GUEST_XREF_CLEAN_FINAL_TEMP SRC 
ON
	DIM.XREF_PROP_ID = SRC.XREF_PROP_ID  
AND DIM.XREF_GUEST_ID = SRC.XREF_GUEST_ID  
AND DIM.XREF_TYPE = SRC.XREF_TYPE  --Included to match Merge statement in PDB_GUEST_XREF_LKP_LOAD_PROC
WHEN NOT MATCHED THEN 
INSERT VALUES
(
	SRC.PRIMARY_PROP_ID,
	SRC.PRIMARY_GUEST_ID,
	SRC.XREF_PROP_ID,
	SRC.XREF_GUEST_ID,
    SRC.PRIMARY_ACCOUNT_NBR,
    SRC.XREF_ACCOUNT_NBR,
    SRC.XREF_TYPE,
    SRC.SOURCE_SYSTEM_NM,
	SRC.TIME_ZONE,
    SRC.CREATED_DTTM,
    SRC.CREATED_BY,
    SRC.UPDATED_DTTM,
    SRC.UPDATED_BY,
    SRC.DELETE_IND,
    SRC.BATCH_ID,
	SRC.GST_ID_XREF_RAW_BATCH_ID,
	'I',
	SRC.KEY_SEQUENCE_NBR
)
WHEN MATCHED THEN 
UPDATE SET 
	DIM.PRIMARY_PROP_ID = SRC.PRIMARY_PROP_ID,
	DIM.PRIMARY_GUEST_ID = SRC.PRIMARY_GUEST_ID,
	DIM.XREF_PROP_ID = SRC.XREF_PROP_ID,
	DIM.XREF_GUEST_ID = SRC.XREF_GUEST_ID,
    DIM.PRIMARY_ACCOUNT_NBR = SRC.PRIMARY_ACCOUNT_NBR,
    DIM.XREF_ACCOUNT_NBR = SRC.XREF_ACCOUNT_NBR,
    DIM.XREF_TYPE = SRC.XREF_TYPE,
    DIM.SOURCE_SYSTEM_NM = SRC.SOURCE_SYSTEM_NM,
	DIM.TIME_ZONE = SRC.TIME_ZONE,
    DIM.UPDATED_DTTM = SRC.UPDATED_DTTM,
    DIM.UPDATED_BY = SRC.UPDATED_BY,
    DIM.DELETE_IND = SRC.DELETE_IND,
    DIM.BATCH_ID = SRC.BATCH_ID,
	DIM.GST_ID_XREF_RAW_BATCH_ID = SRC.GST_ID_XREF_RAW_BATCH_ID,
	DIM.LAST_DML_CD = 'U',
	DIM.KEY_SEQUENCE_NBR = SRC.KEY_SEQUENCE_NBR
;`;	
     
 
my_sql_command_delete_selfreference = 
`delete from  DAAS_CORE.GUEST_XREF_CLEAN_LKP CLEAN_LKP  
where CLEAN_LKP.PRIMARY_ACCOUNT_NBR = CLEAN_LKP.XREF_ACCOUNT_NBR 
and exists 
      (select 1 from DAAS_TEMP.SRC_GUEST_XREF_CLEAN_FINAL_TEMP FINAL_TEMP 
	  where FINAL_TEMP.XREF_ACCOUNT_NBR = CLEAN_LKP.XREF_ACCOUNT_NBR);`; 
     
     
 
     

my_sql_command_last_dml_cd = 
`SELECT NVL(SUM(CASE WHEN LAST_DML_CD = 'U' THEN 1 ELSE 0 END), 0) AS update_count, 
 NVL(SUM(CASE WHEN LAST_DML_CD = 'I' THEN 1 ELSE 0 END), 0) AS insert_count 
 FROM DAAS_CORE.GUEST_XREF_CLEAN_LKP 
 WHERE BATCH_ID = ` + BATCH_ID;

my_sql_command_src_rec_count = `SELECT COUNT(*) AS source_count FROM DAAS_TEMP.GUEST_XREF_TMP`;	
	
try
{
	proc_step = "Data_Process";
	snowflake.execute( {sqlText: "BEGIN;" } );

	var raw_to_temp_load_sql = snowflake.createStatement( {sqlText: my_sql_raw_to_temp_load } );
	proc_output = my_sql_raw_to_temp_load;
	raw_to_temp_load_sql.execute();
	
	var temp_clean_join_sql = snowflake.createStatement( {sqlText: my_sql_command_temp_clean_join } );
	proc_output = my_sql_command_temp_clean_join;
	temp_clean_join_sql.execute();
	
	var my_sql_command_dedup = snowflake.createStatement( {sqlText: my_sql_command_dedup_xref } );
    proc_output = my_sql_command_dedup_xref;
    my_sql_command_dedup.execute();
	
	
	var my_sql_command_infinite_loop_sql = snowflake.createStatement( {sqlText: my_sql_command_infinite_loop } );
    proc_output = my_sql_command_infinite_loop;
    my_sql_command_infinite_loop_sql.execute();
	
	proc_output = my_sql_command_src_count;
    statement_err1 = snowflake.execute( {sqlText: my_sql_command_src_count});
    statement_err1.next();
    count_level = statement_err1.getColumnValue(1);
  
    if ( count_level >=1 )    
    {
	   statement_infinite = snowflake.createStatement( {sqlText:insert_into_infinite_table } );
	   proc_output = insert_into_infinite_table;
	   statement_infinite.execute();
		} 	
 
     
    var my_sql_command_infinite_loop_child_sql = snowflake.createStatement( {sqlText:  my_sql_command_infinite_loop_child } );
    proc_output =  my_sql_command_infinite_loop_child;
    my_sql_command_infinite_loop_child_sql.execute();
 
     
	var fetch_child_data_sql = snowflake.createStatement( {sqlText: my_sql_command_fetch_child_data } );
    proc_output = my_sql_command_fetch_child_data;
    fetch_child_data_sql.execute();
 
	
	var update_DEDUP_TABLE_sql = snowflake.createStatement( {sqlText: my_sql_command_update_DEDUP_TABLE } );
    proc_output = my_sql_command_update_DEDUP_TABLE;
    update_DEDUP_TABLE_sql.execute();
	
		
	var fetch_child_data_level_100_sql= snowflake.createStatement( {sqlText: my_sql_command_fetch_child_data_level_100 } );
	proc_output = my_sql_command_fetch_child_data_level_100;
	fetch_child_data_level_100_sql.execute();	
	 
	 
    var update_child_data_level_100_sql= snowflake.createStatement( {sqlText: my_sql_command_update_child_data_level_100 } );
	proc_output = my_sql_command_update_child_data_level_100;
	update_child_data_level_100_sql.execute();
	
	

	var SRC_GUEST_XREF_CLEAN_FINAL_sql = snowflake.createStatement( {sqlText: my_sql_command_SRC_GUEST_XREF_CLEAN_FINAL } );
	proc_output = my_sql_command_SRC_GUEST_XREF_CLEAN_FINAL;
	SRC_GUEST_XREF_CLEAN_FINAL_sql.execute();

	
	var temp_to_core_load_sql = snowflake.createStatement( {sqlText: my_sql_command_temp_to_core_load } );
	proc_output = my_sql_command_temp_to_core_load;
	temp_to_core_load_sql.execute();
	
    var delete_selfreference = snowflake.createStatement( {sqlText: my_sql_command_delete_selfreference } );
	proc_output = my_sql_command_delete_selfreference;
	delete_selfreference.execute(); 
     
 
	/* Get Insert and update counts */
	
	proc_output = my_sql_command_last_dml_cd;
	var last_dml_cd_sql = snowflake.execute( {sqlText: my_sql_command_last_dml_cd } );
	last_dml_cd_sql.next();
	update_count = last_dml_cd_sql.getColumnValue(1);
	insert_count = last_dml_cd_sql.getColumnValue(2);
	
	proc_output = my_sql_command_src_rec_count;
	var src_rec_count_sql = snowflake.execute( {sqlText: my_sql_command_src_rec_count }); 
	src_rec_count_sql.next();
	src_rec_count = src_rec_count_sql.getColumnValue(1);
		
	snowflake.execute( {sqlText: "COMMIT;" } );
	
	proc_step = "Update_Metrics";
	
	my_sql_command_src_rec_count = "CALL DAAS_COMMON.BATCH_CONTROL_UPDATE_BATCH_METRICS_PROC(" + BATCH_ID + ", '" + SHARD_NAME + "', 'source_record_count', '" + src_rec_count + "')";
	var src_rec_count_sql = snowflake.execute( {sqlText: my_sql_command_src_rec_count });
	src_rec_count_sql.next();
	src_rec_count_update_metric_status = src_rec_count_sql.getColumnValue(1);
	
	my_sql_command_6 = "CALL DAAS_COMMON.BATCH_CONTROL_UPDATE_BATCH_METRICS_PROC(" + BATCH_ID + ", '" + SHARD_NAME + "', 'core_insert_count', '" + insert_count + "')";
	statement6 = snowflake.execute( {sqlText: my_sql_command_6 });
	statement6.next();
	core_insert_count_update_metric_status = statement6.getColumnValue(1);
	
	my_sql_command_7 = "CALL DAAS_COMMON.BATCH_CONTROL_UPDATE_BATCH_METRICS_PROC(" + BATCH_ID + ", '" + SHARD_NAME + "', 'core_update_count', '" + update_count + "')";
	statement7 = snowflake.execute( {sqlText: my_sql_command_7 });
	statement7.next();
	core_update_count_update_metric_status = statement7.getColumnValue(1);
	
	if ( 
		src_rec_count_update_metric_status.includes("SUCCESS") != true ||
		core_insert_count_update_metric_status.includes("SUCCESS") != true ||
		core_update_count_update_metric_status.includes("SUCCESS") != true 
	)
	{
		proc_output = "SOURCE COUNT METRIC STATUS: " + src_rec_count_update_metric_status + "\nCORE INSERT COUNT METRIC STATUS: " + core_insert_count_update_metric_status + "\nCORE UPDATE COUNT METRIC STATUS: " + core_update_count_update_metric_status + "FAILURE RETURNED FROM METRICS";
	}
	
	proc_output = "SUCCESS";
} 
catch (err) 
{ 
	proc_output = "FAILURE";
	error_code = "Failed: Code: " + err.code + "  State: " + err.state;
	error_message = "\n  Message: " + err.message ;
	error_message = "\n  Message: " + err.message + "\nStack Trace:\n" + err.stackTraceTxt;
	error_message = error_message.replace(/["']/g, "");
	if ( proc_step == "Data_Process")
	{
		/*CALL BATCH_CONTROL_UPDATE_BATCH_ERROR_LOG_PROC*/
		snowflake.execute( {sqlText: "ROLLBACK;" } );
		my_sql_command_8 = "CALL DAAS_COMMON.BATCH_CONTROL_UPDATE_BATCH_ERROR_LOG_PROC ('" + BATCH_ID + "','" + SHARD_NAME + "','" + error_code + "','" + error_message + "','','','FATAL','" + tag + "_" + proc_step +"')"	
	}
	else if ( proc_step == "Update_Metrics")
	{
		my_sql_command_8 = "CALL DAAS_COMMON.BATCH_CONTROL_UPDATE_BATCH_ERROR_LOG_PROC ('" + BATCH_ID + "','" + SHARD_NAME + "','" + error_code + "','" + error_message + "','','','INFORMATIONAL','" + tag + "_" + proc_step +"')"
	} 
	snowflake.execute( {sqlText: my_sql_command_8});
}return proc_output ;
$$ ;