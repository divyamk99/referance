CREATE OR REPLACE PROCEDURE DAAS_COMMON.GUEST_XREF_BRIDGE_LKP_LOAD_PROC(BATCH_ID FLOAT, SHARD_NAME VARCHAR, WAREHOUSE_NAME VARCHAR, LEVEL VARCHAR) 
RETURNS VARCHAR(16777216) 
LANGUAGE JAVASCRIPT 
EXECUTE AS CALLER 
AS 
$$ 
/*
#####################################################################################
Author:Suresh V
Purpose: Load data from DAAS_CORE.GUEST_XREF_LKP to DAAS_CORE.GUEST_XREF_BRIDGE_LKP table
Input Parameters: BATCH_ID, SHARD_NAME, WAREHOUSE_NAME, CUSTOM_PARAM1, CUSTOM_PARAM2
Output Value: SUCCESS for successful execution and for failed execution it returns sql statement causing issue with error description
Create Date: 09/16/2022
Modify Date: 10/03/2022
Modified By: Neha Dharaskar
Modify Date: 04/04/2023
Modified By: Rohit Kumar Das
Version: 1.1
#####################################################################################
*/

proc_output = "";
proc_step = "";
snowflake.execute( {sqlText: "USE WAREHOUSE " + WAREHOUSE_NAME} );
tag = BATCH_ID + "_GUEST_XREF_BRIDGE_LKP_LOAD_PROC_ERROR";
snowflake.execute( {sqlText: "ALTER SESSION SET QUERY_TAG = '" + tag + "'" })
;

snowflake.execute( {sqlText: "TRUNCATE TABLE DAAS_TEMP.GUEST_XREF_INCR_TEMP"} );
snowflake.execute( {sqlText: "TRUNCATE TABLE DAAS_TEMP.GUEST_XREF_VICTIMS_INCR_TEMP"} );
snowflake.execute( {sqlText: "TRUNCATE TABLE DAAS_TEMP.GUEST_XREF_ULT_SURVIVOR_INCR_TEMP"} );

/*CAPTURE INCREMENTAL RECORDS FROM BOTH THE STREAMS AND LOAD THE DATA INTO TEMP TABLE*/
capture_incremental_records =`
INSERT INTO DAAS_TEMP.GUEST_XREF_INCR_TEMP
SELECT
	GUEST_DIM.GUEST_UNIQUE_ID AS PRIMARY_ACCOUNT_NBR,
	GUEST_DIM.GUEST_UNIQUE_ID AS XREF_ACCOUNT_NBR,
	GUEST_DIM.SOURCE_SYSTEM_NM,
	GUEST_DIM.TIME_ZONE,
	GUEST_DIM.DELETE_IND AS DELETE_IND
FROM
	DAAS_CORE.GUEST_DIM_BRIDGE_STREAM GUEST_DIM
WHERE
	METADATA$ACTION = 'INSERT'
	--AND DELETE_IND = 'N'
UNION
SELECT
	XREF_CLEAN.PRIMARY_ACCOUNT_NBR,
	XREF_CLEAN.XREF_ACCOUNT_NBR,
	GUEST_DIM.SOURCE_SYSTEM_NM,
	GUEST_DIM.TIME_ZONE,
	GUEST_DIM.DELETE_IND AS DELETE_IND
FROM
	DAAS_CORE.GUEST_DIM_BRIDGE_STREAM GUEST_DIM
LEFT JOIN DAAS_CORE.GUEST_XREF_CLEAN_LKP XREF_CLEAN ON
	GUEST_DIM.GUEST_UNIQUE_ID = XREF_CLEAN.PRIMARY_ACCOUNT_NBR
WHERE
	GUEST_DIM.METADATA$ACTION = 'INSERT'
	AND GUEST_DIM.DELETE_IND = 'N'
	AND XREF_CLEAN.PRIMARY_ACCOUNT_NBR <> XREF_CLEAN.XREF_ACCOUNT_NBR
UNION
SELECT
	XREF_CLEAN.PRIMARY_ACCOUNT_NBR,
	XREF_CLEAN.XREF_ACCOUNT_NBR,
	XREF_CLEAN.SOURCE_SYSTEM_NM,
	XREF_CLEAN.TIME_ZONE,
	XREF_CLEAN.DELETE_IND AS DELETE_IND
FROM
	DAAS_CORE.GUEST_XREF_CLEAN_BRIDGE_STREAM XREF_CLEAN
WHERE
METADATA$ACTION = 'INSERT'
and 
XREF_CLEAN.PRIMARY_ACCOUNT_NBR <> XREF_CLEAN.XREF_ACCOUNT_NBR
`;

/*SELF REFERENCE INSERTS INTO BRIDGE TABLE FROM TEMP TABLE*/	
self_refernce_inserts =`
INSERT
	INTO
	DAAS_CORE.GUEST_XREF_BRIDGE_LKP
SELECT
	INCR_TEMP.PRIMARY_ACCOUNT_NBR,
	INCR_TEMP.XREF_ACCOUNT_NBR,
	INCR_TEMP.SOURCE_SYSTEM_NM,
	INCR_TEMP.TIME_ZONE,
	CURRENT_TIMESTAMP() AS CREATED_DTTM,
	CURRENT_USER() AS CREATED_BY ,
	CURRENT_TIMESTAMP() AS UPDATED_DTTM,
	CURRENT_USER() AS UPDATED_BY,
	`+ BATCH_ID + ` AS BATCH_ID,
	'GUEST_XREF' AS MAPPING_SOURCE,
	0 AS REPLAY_COUNTER,
	'Y' AS ULTIMATE_SURVIVOR_FLG
FROM
	DAAS_TEMP.GUEST_XREF_INCR_TEMP INCR_TEMP
LEFT JOIN DAAS_CORE.GUEST_XREF_BRIDGE_LKP XREF_BRG ON
	INCR_TEMP.PRIMARY_ACCOUNT_NBR = XREF_BRG.XREF_ACCOUNT_NBR
WHERE
	XREF_BRG.PRIMARY_ACCOUNT_NBR IS NULL
	AND INCR_TEMP.DELETE_IND = 'N' --ADDED THIS CONDITION TO CAPTURE SELF REFERENCING RECORDS
	AND INCR_TEMP.PRIMARY_ACCOUNT_NBR = INCR_TEMP.XREF_ACCOUNT_NBR
`;
/*IDENTIFY ALL IMPACTED XREF_NBRS*/

identify_impacted_victims =`
INSERT INTO DAAS_TEMP.GUEST_XREF_VICTIMS_INCR_TEMP
	WITH RECURSIVE CTE_XREF_VICTIMS (
	LEVEL,
	PRIMARY_ACCOUNT_NBR,
	XREF_ACCOUNT_NBR,
	SOURCE_SYSTEM_NM,
	TIME_ZONE
	)
    AS (
	SELECT
		0 LEVEL,
		CASE WHEN XREF_INCR_TEMP.DELETE_IND = 'Y' THEN NULL ELSE XREF_INCR_TEMP.PRIMARY_ACCOUNT_NBR END AS PRIMARY_ACCOUNT_NBR,
		XREF_INCR_TEMP.XREF_ACCOUNT_NBR,
		XREF_INCR_TEMP.SOURCE_SYSTEM_NM,
		XREF_INCR_TEMP.TIME_ZONE
		
	FROM
		DAAS_TEMP.GUEST_XREF_INCR_TEMP XREF_INCR_TEMP
	WHERE
	XREF_INCR_TEMP.PRIMARY_ACCOUNT_NBR <> XREF_INCR_TEMP.XREF_ACCOUNT_NBR
	
UNION ALL
	SELECT
	LEVEL + 1 AS LEVEL,
	XREF_CLEAN.PRIMARY_ACCOUNT_NBR,
	XREF_CLEAN.XREF_ACCOUNT_NBR,
	XREF_CLEAN.SOURCE_SYSTEM_NM,
	XREF_CLEAN.TIME_ZONE
FROM
	DAAS_CORE.GUEST_XREF_CLEAN_LKP XREF_CLEAN,
	CTE_XREF_VICTIMS CTE
WHERE
	CTE.XREF_ACCOUNT_NBR = XREF_CLEAN.PRIMARY_ACCOUNT_NBR
	AND XREF_CLEAN.PRIMARY_ACCOUNT_NBR <> XREF_CLEAN.XREF_ACCOUNT_NBR
	AND XREF_CLEAN.DELETE_IND = 'N'
	AND LEVEL < `+LEVEL +`)
SELECT DISTINCT LEVEL, PRIMARY_ACCOUNT_NBR, XREF_ACCOUNT_NBR, SOURCE_SYSTEM_NM, TIME_ZONE FROM CTE_XREF_VICTIMS`;
	
/*-----------------------------------------------------------------------------------------------------
--VALIDATION IF ANY RECORD IN TEMP TABLE HAS LEVEL >=100 THEN LOG INTO BATCH ERROR LOG TABLE, SEND AN ALERT AND FAIL THE PROCESS  
-----------------------------------------------------------------------------------------------------*/
/*infinite_loop_error_victims =`SELECT COUNT(*) AS COUNT_LEVEL FROM DAAS_TEMP.GUEST_XREF_VICTIMS_INCR_TEMP WHERE LEVEL >=`+LEVEL +``;
insert_into_infinite_table=`
INSERT INTO DAAS_CORE.XREF_INFINITE_RECORDS 
	 ( 
	SELECT 
	INFINITE_RECORDS.LEVEL AS LEVEL,
	INFINITE_RECORDS.PRIMARY_ACCOUNT_NBR,
	INFINITE_RECORDS.XREF_ACCOUNT_NBR,
	INFINITE_RECORDS.SOURCE_SYSTEM_NM,
	INFINITE_RECORDS.TIME_ZONE,
	`+ BATCH_ID + ` AS BATCH_ID,
	CURRENT_TIMESTAMP() AS CREATED_DTTM,
	CURRENT_USER() AS CREATED_BY,
	CURRENT_TIMESTAMP() AS UPDATED_DTTM,
	CURRENT_USER() AS UPDATED_BY,
	'INFINITE' AS ALERT_TYPE
	FROM DAAS_TEMP.GUEST_XREF_VICTIMS_INCR_TEMP INFINITE_RECORDS
	WHERE INFINITE_RECORDS.LEVEL =`+LEVEL +`)`;
*/				
/*IDENTIFY ALL ULTIMATE PRIMARY_NBRS*/

indentify_ultimate_survivors= `
INSERT INTO DAAS_TEMP.GUEST_XREF_ULT_SURVIVOR_INCR_TEMP
WITH RECURSIVE CTE_XREF_SURVIVORS 
    AS (
SELECT
    0 LEVEL ,
    VICTIM_TEMP.PRIMARY_ACCOUNT_NBR,
    VICTIM_TEMP.XREF_ACCOUNT_NBR,
    VICTIM_TEMP.SOURCE_SYSTEM_NM,
    VICTIM_TEMP.TIME_ZONE
FROM
    DAAS_TEMP.GUEST_XREF_VICTIMS_INCR_TEMP VICTIM_TEMP
	WHERE VICTIM_TEMP.LEVEL < `+LEVEL +`
UNION ALL
SELECT
    LEVEL + 1 ,
    XREF_CLEAN.PRIMARY_ACCOUNT_NBR,
    CTE.XREF_ACCOUNT_NBR,
    XREF_CLEAN.SOURCE_SYSTEM_NM,
    XREF_CLEAN.TIME_ZONE
    
FROM
    DAAS_CORE.GUEST_XREF_CLEAN_LKP XREF_CLEAN,
    CTE_XREF_SURVIVORS CTE
WHERE
    CTE.PRIMARY_ACCOUNT_NBR = XREF_CLEAN.XREF_ACCOUNT_NBR
    AND XREF_CLEAN.XREF_ACCOUNT_NBR <> XREF_CLEAN.PRIMARY_ACCOUNT_NBR
    AND XREF_CLEAN.DELETE_IND = 'N'
    AND LEVEL < `+LEVEL +`)
SELECT
    LEVEL,
    CASE WHEN GUEST_DIM.GUEST_UNIQUE_ID IS NULL THEN -1 else CTE.PRIMARY_ACCOUNT_NBR END AS PRIMARY_ACCOUNT_NBR,
    CTE.XREF_ACCOUNT_NBR,
    CTE.SOURCE_SYSTEM_NM,
    CTE.TIME_ZONE,
	CASE WHEN GUEST_DIM.GUEST_UNIQUE_ID IS NULL AND CTE.PRIMARY_ACCOUNT_NBR <> -1 THEN 'GUEST DOES NOT EXIST IN GUEST DIM' 
	WHEN LEVEL =`+LEVEL +` THEN 'INFINITE'   
	ELSE NULL END AS ALERT_TYPE
FROM CTE_XREF_SURVIVORS CTE
    LEFT OUTER JOIN (SELECT DISTINCT GUEST_UNIQUE_ID FROM DAAS_CORE.GUEST_DIM WHERE DELETE_IND = 'N' ) GUEST_DIM ON GUEST_DIM.GUEST_UNIQUE_ID = CTE.PRIMARY_ACCOUNT_NBR
    QUALIFY ROW_NUMBER() OVER (PARTITION BY XREF_ACCOUNT_NBR
ORDER BY
    LEVEL DESC) = 1`;
/*-----------------------------------------------------------------------------------------------------
--VALIDATION IF ANY RECORD IN TEMP TABLE HAS LEVEL >=100 THEN LOG INTO BATCH ERROR LOG TABLE, SEND AN ALERT AND FAIL THE PROCESS 
-----------------------------------------------------------------------------------------------------*/

infinite_loop_error_ult=`SELECT COUNT(*) AS COUNT_LEVEL FROM DAAS_TEMP.GUEST_XREF_ULT_SURVIVOR_INCR_TEMP WHERE ALERT_TYPE IN ('INFINITE','GUEST DOES NOT EXIST IN GUEST DIM')`;
insert_into_infinite_ult_table=`
INSERT INTO DAAS_CORE.XREF_ALERT_RECORDS
	 ( 
	SELECT 
	INFINITE_RECORDS.LEVEL AS LEVEL,
	INFINITE_RECORDS.PRIMARY_ACCOUNT_NBR,
	INFINITE_RECORDS.XREF_ACCOUNT_NBR,
	INFINITE_RECORDS.SOURCE_SYSTEM_NM,
	INFINITE_RECORDS.TIME_ZONE,
	`+ BATCH_ID + ` AS BATCH_ID,
	CURRENT_TIMESTAMP() AS CREATED_DTTM,
	CURRENT_USER() AS CREATED_BY,
	CURRENT_TIMESTAMP() AS UPDATED_DTTM,
	CURRENT_USER() AS UPDATED_BY,
	INFINITE_RECORDS.ALERT_TYPE AS ALERT_TYPE
	FROM DAAS_TEMP.GUEST_XREF_ULT_SURVIVOR_INCR_TEMP INFINITE_RECORDS
	WHERE ALERT_TYPE IN ('INFINITE','GUEST DOES NOT EXIST IN GUEST DIM'))`;

/*MERGE INTO GUEST XREF BRIDGE TABLE*/
merge_into_bridge_tbl =`
MERGE
INTO
	DAAS_CORE.GUEST_XREF_BRIDGE_LKP AS TGT
		USING (
	SELECT
		PRIMARY_ACCOUNT_NBR,
		XREF_ACCOUNT_NBR,
		SOURCE_SYSTEM_NM,
		TIME_ZONE,
		CURRENT_TIMESTAMP() AS CREATED_DTTM,
		CURRENT_USER() AS CREATED_BY ,
		CURRENT_TIMESTAMP() AS UPDATED_DTTM,
		CURRENT_USER() AS UPDATED_BY,
		`+ BATCH_ID + ` AS BATCH_ID,
		'GUEST_XREF' AS MAPPING_SOURCE,
		CASE WHEN PRIMARY_ACCOUNT_NBR = XREF_ACCOUNT_NBR THEN 'Y'
		ELSE 'N' END AS ULTIMATE_SURVIVOR_FLG		
	FROM
		DAAS_TEMP.GUEST_XREF_ULT_SURVIVOR_INCR_TEMP
		WHERE LEVEL < `+LEVEL +`) AS TEMP ON
	TEMP.XREF_ACCOUNT_NBR = TGT.XREF_ACCOUNT_NBR
	WHEN MATCHED AND TEMP.PRIMARY_ACCOUNT_NBR <> TGT.PRIMARY_ACCOUNT_NBR THEN UPDATE SET TGT.PRIMARY_ACCOUNT_NBR = TEMP.PRIMARY_ACCOUNT_NBR, 
	TGT.UPDATED_DTTM = CURRENT_TIMESTAMP(), TGT.UPDATED_BY = CURRENT_USER(), TGT.BATCH_ID = `+ BATCH_ID + `,TGT.ULTIMATE_SURVIVOR_FLG= TEMP.ULTIMATE_SURVIVOR_FLG
	WHEN MATCHED AND TEMP.PRIMARY_ACCOUNT_NBR <> TGT.PRIMARY_ACCOUNT_NBR AND TEMP.PRIMARY_ACCOUNT_NBR IS NULL THEN 
	UPDATE SET TGT.PRIMARY_ACCOUNT_NBR = TEMP.XREF_ACCOUNT_NBR, TGT.UPDATED_DTTM = CURRENT_TIMESTAMP(), TGT.UPDATED_BY = CURRENT_USER(), 
	TGT.BATCH_ID = `+ BATCH_ID + `, TGT.ULTIMATE_SURVIVOR_FLG= TEMP.ULTIMATE_SURVIVOR_FLG
	WHEN NOT MATCHED THEN INSERT (PRIMARY_ACCOUNT_NBR, XREF_ACCOUNT_NBR, SOURCE_SYSTEM_NM, TIME_ZONE, CREATED_DTTM, CREATED_BY, UPDATED_DTTM, UPDATED_BY, BATCH_ID, MAPPING_SOURCE,ULTIMATE_SURVIVOR_FLG)
	VALUES (TEMP.PRIMARY_ACCOUNT_NBR, TEMP.XREF_ACCOUNT_NBR, TEMP.SOURCE_SYSTEM_NM,	TEMP.TIME_ZONE,	TEMP.CREATED_DTTM, TEMP.CREATED_BY,	TEMP.UPDATED_DTTM, TEMP.UPDATED_BY,	TEMP.BATCH_ID, TEMP.MAPPING_SOURCE,TEMP.ULTIMATE_SURVIVOR_FLG)
	`;

/*UPDATE BRIDGE TABLE TO PROCESS SOFT DELETES IN GUEST_DIM TABLE*/
update_bridge_tbl_for_soft_deletes = `
UPDATE DAAS_CORE.GUEST_XREF_BRIDGE_LKP TGT SET TGT.PRIMARY_ACCOUNT_NBR = -1,
TGT.ULTIMATE_SURVIVOR_FLG = 'N'
FROM(
SELECT DISTINCT B.PRIMARY_ACCOUNT_NBR , B.XREF_ACCOUNT_NBR, B.ULTIMATE_SURVIVOR_FLG FROM DAAS_TEMP.GUEST_XREF_INCR_TEMP A
INNER JOIN DAAS_CORE.GUEST_XREF_BRIDGE_LKP B
ON B.XREF_ACCOUNT_NBR = A.XREF_ACCOUNT_NBR
WHERE A.DELETE_IND = 'Y' AND
A.PRIMARY_ACCOUNT_NBR = A.XREF_ACCOUNT_NBR
AND B.ULTIMATE_SURVIVOR_FLG = 'Y'
) SRC
WHERE TGT.XREF_ACCOUNT_NBR = SRC.XREF_ACCOUNT_NBR AND TGT.PRIMARY_ACCOUNT_NBR <> -1
;`;


 update_bridge_tbl_for_selfentry =`
 UPDATE  DAAS_CORE.GUEST_XREF_BRIDGE_LKP B  SET B.PRIMARY_ACCOUNT_NBR = B.XREF_ACCOUNT_NBR ,
 B.ULTIMATE_SURVIVOR_FLG='Y'
 FROM  DAAS_TEMP.GUEST_XREF_ULT_SURVIVOR_INCR_TEMP  TMP
 WHERE TMP.PRIMARY_ACCOUNT_NBR = B.XREF_ACCOUNT_NBR AND B.PRIMARY_ACCOUNT_NBR = -1`;
    
 	
insert_count_into_bridge_tbl = `SELECT COUNT(*) AS insert_count FROM DAAS_CORE.GUEST_XREF_BRIDGE_LKP WHERE BATCH_ID = ` + BATCH_ID;
source_count = `SELECT COUNT(*) AS source_count FROM DAAS_TEMP.GUEST_XREF_ULT_SURVIVOR_INCR_TEMP`;
update_count_brg= `SELECT COUNT(*) AS update_count FROM DAAS_CORE.GUEST_XREF_BRIDGE_LKP WHERE UPDATED_DTTM <> CREATED_DTTM AND BATCH_ID =` + BATCH_ID;
 
try
{
	proc_step = "Data_Process";
	snowflake.execute( {sqlText: "BEGIN;" } );

	statement1 = snowflake.createStatement( {sqlText: capture_incremental_records } );
	proc_output = capture_incremental_records;
	statement1.execute();
	
	statement2 = snowflake.createStatement( {sqlText: self_refernce_inserts } );
	proc_output = self_refernce_inserts;
	statement2.execute();
	
		
	statement3 = snowflake.createStatement( {sqlText: identify_impacted_victims } );
	proc_output = identify_impacted_victims;
	statement3.execute();
	
				
	statement4 = snowflake.createStatement( {sqlText: indentify_ultimate_survivors } );
	proc_output = indentify_ultimate_survivors;
	statement4.execute();
	
	proc_output = infinite_loop_error_ult;
    statement_err2 = snowflake.execute( {sqlText: infinite_loop_error_ult});
    statement_err2.next();
    count_level_2 = statement_err2.getColumnValue(1);
	
	if ( count_level_2 >=1 )    
    {
	   statement_infinite_ult = snowflake.createStatement( {sqlText:insert_into_infinite_ult_table } );
	   proc_output = insert_into_infinite_ult_table;
	   statement_infinite_ult.execute();
		   /* var code= "999999"+"_"+BATCH_ID;
			var state= "PARTIAL_SUCCESS";
			var message ="FOR BATCH_ID:"+BATCH_ID+" "+"INFINITE RECORDS ARE PRESENT IN INCREMENTAL WHILE FINDING THE ULTIMATE SURVIVOR. REFER DAAS_CORE.XREF_INFINITE_RECORDS TABLE FOR MORE DETAILS FOR THIS BATCH ID";
	        my_sql_command_partial = "CALL DAAS_COMMON.BATCH_CONTROL_UPDATE_BATCH_ERROR_LOG_PROC ('" + BATCH_ID + "','" + SHARD_NAME + "','" + code + "','" + message + "','','','INFORMATIONAL','" + tag + "_" + proc_step +"')"
	        snowflake.execute( {sqlText: my_sql_command_partial}); */
		} 
		
    statement5 = snowflake.createStatement( {sqlText: merge_into_bridge_tbl } );
	proc_output = merge_into_bridge_tbl;
	statement5.execute();
    
    statement6 = snowflake.createStatement( {sqlText: update_bridge_tbl_for_soft_deletes } );
	proc_output = update_bridge_tbl_for_soft_deletes;
	statement6.execute();

    statement7 = snowflake.createStatement( {sqlText: update_bridge_tbl_for_selfentry } );
	proc_output = update_bridge_tbl_for_selfentry;
	statement7.execute();
	
	/* Get Insert and update counts */
    
	proc_output = insert_count_into_bridge_tbl;
	statement_insert_cnt = snowflake.execute( {sqlText: insert_count_into_bridge_tbl } );
	statement_insert_cnt.next();
	insert_count_bridge = statement_insert_cnt.getColumnValue(1);
	
	proc_output = source_count;
	statement_source_cnt = snowflake.execute( {sqlText: source_count } );
	statement_source_cnt.next();
	source_count_bridge = statement_source_cnt.getColumnValue(1);
	
	proc_output = update_count_brg;
	statement_update_cnt = snowflake.execute( {sqlText: update_count_brg } );
	statement_update_cnt.next();
	update_count_bridge = statement_update_cnt.getColumnValue(1);
		
	snowflake.execute( {sqlText: "COMMIT;" } );
	
	proc_step = "Update_Metrics";
	
	source_bridge_tbl = "CALL DAAS_COMMON.BATCH_CONTROL_UPDATE_BATCH_METRICS_PROC(" + BATCH_ID + ", '" + SHARD_NAME + "', 'source_record_count', '" + source_count_bridge + "')";
	statement_sr_cnt = snowflake.execute( {sqlText: source_bridge_tbl });
	statement_sr_cnt.next();
	src_rec_count_update_metric_status = statement_sr_cnt.getColumnValue(1);
	
	insert_bridge_tbl = "CALL DAAS_COMMON.BATCH_CONTROL_UPDATE_BATCH_METRICS_PROC(" + BATCH_ID + ", '" + SHARD_NAME + "', 'core_insert_count', '" + insert_count_bridge + "')";
	statement_insrt_cnt1 = snowflake.execute( {sqlText: insert_bridge_tbl });
	statement_insrt_cnt1.next();
	core_insert_count_update_metric_status = statement_insrt_cnt1.getColumnValue(1);
	
	update_bridge_tbl = "CALL DAAS_COMMON.BATCH_CONTROL_UPDATE_BATCH_METRICS_PROC(" + BATCH_ID + ", '" + SHARD_NAME + "', 'core_update_count', '" + update_count_bridge + "')";
	statement_updt_cnt = snowflake.execute( {sqlText: update_bridge_tbl });
	statement_updt_cnt.next();
	core_update_count_update_metric_status = statement_updt_cnt.getColumnValue(1); 
	
	if ( 
		src_rec_count_update_metric_status !== "SUCCESS" || 
		core_insert_count_update_metric_status !== "SUCCESS" ||
		core_update_count_update_metric_status !== "SUCCESS" 
	)
	{
		proc_output = "SOURCE COUNT METRIC STATUS: " + src_rec_count_update_metric_status + "\nCORE INSERT COUNT METRIC STATUS: " + core_insert_count_update_metric_status +  "\nCORE UPDATE COUNT METRIC STATUS: " + core_update_count_update_metric_status + "FAILURE RETURNED FROM METRICS";
	}
	
	/* if (count_level >=1 || count_level_2 >=1) 
	{
	proc_output = "PARTIAL_SUCCESS";
	}
	else */ 
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
}
return proc_output ;
$$
; 