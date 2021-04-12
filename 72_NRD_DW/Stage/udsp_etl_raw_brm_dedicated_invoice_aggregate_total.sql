CREATE OR REPLACE PROCEDURE `rax-staging-dev.stage_one.udsp_etl_raw_brm_dedicated_invoice_aggregate_total`()
BEGIN
-----------------------------------------------------------------------------------------------------------------

create or replace temp table Dedicated_stage as 
select --#Dedicated_stage
  ACCOUNT_POID_ID0,
BRM_ACCOUNTNO,
ACCOUNT_ID,
COMPANY_NAME,
GL_SEGMENT,
BILL_POID_ID0,
AR_BILL_OBJ_ID0,
BILL_NO,
Bill_ACCOUNT_POID_ID0,
Bill_BRM_ACCOUNTNO,
Bill_ACCOUNT_ID,
case when Bill_ACCOUNT_ID=ACCOUNT_ID
	then COMPANY_NAME else Bill_COMPANY_NAME
end as Bill_COMPANY_NAME,
Bill_GL_SEGMENT,   
Manual_BILL_NO,
BILL_START_DATE,
BILL_END_DATE,
GL_Time_Month_Key,
Time_Month_Key,
BILL_MOD_DATE,
CURRENT_TOTAL,
TOTAL_DUE,
Bill_Type,
Bill_Source,
`Exclude`,
LINE_OF_BUSINESS,
case when Bill_ACCOUNT_ID=ACCOUNT_ID
	then LINE_OF_BUSINESS else Bill_LINE_OF_BUSINESS
end as 	Bill_LINE_OF_BUSINESS,
Bill_Created_Date
from(
SELECT DISTINCT --INTO    #Dedicated_stage
    ACCOUNT_POID_ID0,
    BRM_ACCOUNTNO,
    ACCOUNT_NUMBER																 AS ACCOUNT_ID,
    AcctB.COMPANY_NAME,
    AcctB.GL_SEGMENT,
    BILL_POID_ID0,
    AR_BILL_OBJ_ID0,
    BILL_NO,
    Bill_ACCOUNT_POID_ID0,
    Bill_BRM_ACCOUNTNO,
    ACCOUNT_NUMBER																 AS Bill_ACCOUNT_ID,
    CAST('Unknown' as string)												 AS Bill_COMPANY_NAME,
    Bill_GL_SEGMENT,   
    Manual_BILL_NO,
    BILL_START_DATE,
    BILL_END_DATE,
    GL_Time_Month_Key,
    Time_Month_Key,
    BILL_MOD_DATE,
    CURRENT_TOTAL,
    TOTAL_DUE,
    Bill_Type,
    Bill_Source,
    0				   AS `Exclude`,
    AcctB.LINE_OF_BUSINESS,
    AcctB.LINE_OF_BUSINESS  AS Bill_LINE_OF_BUSINESS,
	Bill_Created_Date  

FROM 
   (
		   SELECT   --INTO   #dedicated_Invoicedata
			GL_ACCOUNT_POID_ID0														  AS ACCOUNT_POID_ID0,
			CAST(GL_account_no as STRING) 										  AS BRM_ACCOUNTNO,
			GL_SEGMENT																  AS GL_SEGMENT,
			BILL_POID_ID0,
			AR_BILL_OBJ_ID0,
			CAST(BILL_NO as STRING)												  AS BILL_NO,
			Bill_ACCOUNT_POID_ID0,
			CAST(Bill_account_no as STRING) 										  AS Bill_BRM_ACCOUNTNO,
			Bill_GL_SEGMENT,   
			CAST(BILL_NO as STRING)												  AS Manual_BILL_NO,
			cast(TIMESTAMP_ADD(TIMESTAMP("1970-01-01 00:00:00"), INTERVAL cast(start_t  as int64) second)  as datetime)  AS BILL_START_DATE,
			cast(TIMESTAMP_ADD(TIMESTAMP("1970-01-01 00:00:00"), INTERVAL cast(end_t  as int64) second)  as datetime) AS BILL_END_DATE,
			GL_Time_Month_Key,
			bq_functions.udf_yearmonth_nohyphen(cast(TIMESTAMP_ADD(TIMESTAMP("1970-01-01 00:00:00"), INTERVAL cast(end_t  as int64) second)  as datetime)) AS Time_Month_Key,
			cast(TIMESTAMP_ADD(TIMESTAMP("1970-01-01 00:00:00"), INTERVAL cast(mod_t  as int64) second)  as datetime) AS BILL_MOD_DATE,
			TRUNC(cast(CURRENT_TOTAL	AS NUMERIC),2)									  AS CURRENT_TOTAL,
			TRUNC(cast(TOTAL_DUE 	AS NUMERIC),2)											  AS TOTAL_DUE,
			CAST(POID_TYPE	as STRING)											  AS Bill_Type,
			CAST('bill_t' as STRING)											  AS Bill_Source,
			Bill_Created_Date
		FROM 
		   (
		SELECT 
			AggAct.poid_id0			  AS GL_ACCOUNT_POID_ID0,
			AggAct.account_no		  AS GL_account_no,
			AggAct.GL_SEGMENT		  AS GL_SEGMENT,
			Bill.poid_id0			  AS BILL_POID_ID0,
			A.AR_BILL_OBJ_ID0,
			A.Time_Month_Key		  AS GL_Time_Month_Key,
			BILL_NO,
			ITEM_TOTAL				  AS CURRENT_TOTAL,
			billtAct.poid_id0		  AS Bill_ACCOUNT_POID_ID0,
			billtAct.account_no		  AS Bill_account_no,
			billtAct.GL_SEGMENT		  AS Bill_GL_SEGMENT,       
			Bill.start_t			  AS start_t,
			Bill.mod_t				  AS mod_t,
			Bill.end_t				  AS end_t,	
			Bill.POID_TYPE			  AS POID_TYPE,
			IFNULL(CURRENT_TOTAL,0)	  AS Bill_CURRENT_TOTAL,
			IFNULL(TOTAL_DUE,0)		  AS TOTAL_DUE,
			Bill.Created_T			  AS Bill_Created_Date
		FROM 
			stage_one.raw_dedicated_brm_items_aggregate  A    
		INNER JOIN
			`rax-landing-qa`.brm_ods.account_t AggAct
		ON A.ACCOUNT_OBJ_ID0= AggAct.Poid_Id0               
		INNER JOIN 
			 `rax-landing-qa`.brm_ods.bill_t  Bill 
		ON  Bill.poid_id0 =A.Bill_Obj_Id0   
		INNER JOIN
			`rax-landing-qa`.brm_ods.account_t billtAct
		ON Bill.ACCOUNT_OBJ_ID0= billtAct.Poid_Id0   
		) 
   ) A --#dedicated_Invoicedata A  
LEFT OUTER JOIN
  stage_one.raw_brm_dedicated_account_profile  AcctB
ON A.ACCOUNT_POID_ID0=AcctB.ACCOUNT_POID
WHERE
   (upper(AcctB.CONTACT_TYPE)='PRIMARY_CONTACT')
   
union all
SELECT DISTINCT --#Dedicated_stage
    ACCOUNT_POID_ID0,
    BRM_ACCOUNTNO,
    ACCOUNT_NUMBER									     AS ACCOUNT_ID,
    AcctB.COMPANY_NAME,
    AcctB.GL_SEGMENT,
    BILL_POID_ID0,
    AR_BILL_OBJ_ID0,
    BILL_NO,
    Bill_ACCOUNT_POID_ID0,
    Bill_BRM_ACCOUNTNO,
    ACCOUNT_NUMBER									    AS Bill_ACCOUNT_ID,
    CAST('Unknown' as string)					    AS Bill_COMPANY_NAME,
    Bill_GL_SEGMENT,   
    Manual_BILL_NO,
    BILL_START_DATE,
    BILL_END_DATE,
    GL_Time_Month_Key,
    Time_Month_Key,
    BILL_MOD_DATE,
    CURRENT_TOTAL,
    TOTAL_DUE,
    Bill_Type,
    Bill_Source,
    0				   AS `Exclude`,
    AcctB.LINE_OF_BUSINESS,
    AcctB.LINE_OF_BUSINESS  AS Bill_LINE_OF_BUSINESS,
	Bill_Created_Date  
FROM 
   (
		  
				   SELECT   --INTO   #dedicated_Invoicedata
					GL_ACCOUNT_POID_ID0														  AS ACCOUNT_POID_ID0,
					CAST(GL_account_no as STRING) 										  AS BRM_ACCOUNTNO,
					GL_SEGMENT																  AS GL_SEGMENT,
					BILL_POID_ID0,
					AR_BILL_OBJ_ID0,
					CAST(BILL_NO as STRING)												  AS BILL_NO,
					Bill_ACCOUNT_POID_ID0,
					CAST(Bill_account_no as STRING) 										  AS Bill_BRM_ACCOUNTNO,
					Bill_GL_SEGMENT,   
					CAST(BILL_NO as STRING)												  AS Manual_BILL_NO,
					cast(TIMESTAMP_ADD(TIMESTAMP("1970-01-01 00:00:00"), INTERVAL cast(start_t  as int64) second)  as datetime)  AS BILL_START_DATE,
					cast(TIMESTAMP_ADD(TIMESTAMP("1970-01-01 00:00:00"), INTERVAL cast(end_t  as int64) second)  as datetime) AS BILL_END_DATE,
					GL_Time_Month_Key,
					bq_functions.udf_yearmonth_nohyphen(cast(TIMESTAMP_ADD(TIMESTAMP("1970-01-01 00:00:00"), INTERVAL cast(end_t  as int64) second)  as datetime)) AS Time_Month_Key,
					cast(TIMESTAMP_ADD(TIMESTAMP("1970-01-01 00:00:00"), INTERVAL cast(mod_t  as int64) second)  as datetime) AS BILL_MOD_DATE,
					TRUNC(cast(CURRENT_TOTAL	AS NUMERIC),2)									  AS CURRENT_TOTAL,
					TRUNC(cast(TOTAL_DUE 	AS NUMERIC),2)											  AS TOTAL_DUE,
					CAST(POID_TYPE	as STRING)											  AS Bill_Type,
					CAST('bill_t' as STRING)											  AS Bill_Source,
					Bill_Created_Date
				FROM 
				   (
				SELECT 
					AggAct.poid_id0			  AS GL_ACCOUNT_POID_ID0,
					AggAct.account_no		  AS GL_account_no,
					AggAct.GL_SEGMENT		  AS GL_SEGMENT,
					Bill.poid_id0			  AS BILL_POID_ID0,
					A.AR_BILL_OBJ_ID0,
					A.Time_Month_Key		  AS GL_Time_Month_Key,
					BILL_NO,
					ITEM_TOTAL				  AS CURRENT_TOTAL,
					billtAct.poid_id0		  AS Bill_ACCOUNT_POID_ID0,
					billtAct.account_no		  AS Bill_account_no,
					billtAct.GL_SEGMENT		  AS Bill_GL_SEGMENT,       
					Bill.start_t			  AS start_t,
					Bill.mod_t				  AS mod_t,
					Bill.end_t				  AS end_t,	
					Bill.POID_TYPE			  AS POID_TYPE,
					IFNULL(CURRENT_TOTAL,0)	  AS Bill_CURRENT_TOTAL,
					IFNULL(TOTAL_DUE,0)		  AS TOTAL_DUE,
					Bill.Created_T			  AS Bill_Created_Date
				FROM 
					stage_one.raw_dedicated_brm_items_aggregate  A    
				INNER JOIN
					`rax-landing-qa`.brm_ods.account_t AggAct
				ON A.ACCOUNT_OBJ_ID0= AggAct.Poid_Id0               
				INNER JOIN 
					 `rax-landing-qa`.brm_ods.bill_t  Bill 
				ON  Bill.poid_id0 =A.Bill_Obj_Id0   
				INNER JOIN
					`rax-landing-qa`.brm_ods.account_t billtAct
				ON Bill.ACCOUNT_OBJ_ID0= billtAct.Poid_Id0   
				) 
		   
   ) A --#dedicated_Invoicedata A  
LEFT OUTER JOIN
  stage_one.raw_brm_dedicated_account_profile  AcctB
ON A.ACCOUNT_POID_ID0=AcctB.ACCOUNT_POID
WHERE
   (upper(AcctB.CONTACT_TYPE)='BILLING')

);
-----------------------------------------------------------------------------------------------------------------

-----------------------------------------------------------------------------------------------------------------
UPDATE Dedicated_stage A
SET
    Bill_ACCOUNT_ID=ACCOUNT_NUMBER,
    Bill_COMPANY_NAME=AcctB.COMPANY_NAME,
    Bill_LINE_OF_BUSINESS=AcctB.LINE_OF_BUSINESS
FROm 
   stage_one.raw_brm_cloud_account_profile  AcctB
WHERE A.Bill_ACCOUNT_POID_ID0=AcctB.ACCOUNT_POID
AND lower(Bill_GL_SEGMENT)='.cloud'
AND lower(Bill_COMPANY_NAME)='unknown';


-----------------------------------------------------------------------------------------------------------------
create or replace table stage_one.raw_brm_dedicated_invoice_aggregate_total as 
SELECT
    ACCOUNT_POID_ID0,
    BRM_ACCOUNTNO,
    ACCOUNT_ID,
    COMPANY_NAME,
    GL_SEGMENT,
    BILL_POID_ID0,
    AR_BILL_OBJ_ID0,
    BILL_NO,
    Bill_ACCOUNT_POID_ID0,
    Bill_BRM_ACCOUNTNO,
    Bill_ACCOUNT_ID,
    Bill_COMPANY_NAME,
    Bill_GL_SEGMENT,   
    Manual_BILL_NO,
    BILL_START_DATE,
    BILL_END_DATE,
    GL_Time_Month_Key,
    Time_Month_Key,
    BILL_MOD_DATE,
    CURRENT_TOTAL,
    TOTAL_DUE,
    Bill_Type,
    Bill_Source,
    `Exclude`,
    LINE_OF_BUSINESS,
    Bill_LINE_OF_BUSINESS ,
    current_datetime()					 AS Tbl_Load_Date,
	Bill_Created_Date
FROM Dedicated_stage;

END;
