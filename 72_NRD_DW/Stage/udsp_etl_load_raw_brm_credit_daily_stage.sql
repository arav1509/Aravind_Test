CREATE OR REPLACE PROCEDURE `rax-staging-dev.stage_one.udsp_etl_load_raw_brm_credit_daily_stage`()
BEGIN

/*====================================================================================================================================================================================================
Created On: 10/14/2019
Created By: hari4586
Description:  Script to load BRM daily credits stage
      Modified By    Date     Description
1)
====================================================================================================================================================================================================*/
DECLARE jobdate datetime;
DECLARE GETDATE DATETIME;
DECLARE GETDATE_UNIX  int64;
DECLARE SQL string;
---------------------------------------------------------------------------------------
SET jobdate =(SELECT Max(Credit_MOD_DATE) FROM stage_two_dw.stage_credits_brm );
--Stage_Two_DW].dbo.Stage_Credits_BRM with(nolock))

SET GETDATE_UNIX = UNIX_SECONDS(cast(jobdate as  TIMESTAMP)) ;


create or replace table stage_one.raw_credits_brm_daily_stage as 
SELECT
    BRM_Account_No
    ,Account
    ,AccountName
    ,EBI_GL_ID
     ,GL_Segment
    ,Event_POID_ID0
    ,Item_POID_ID0
    ,Item_POID_Type
    ,BILL_OBJ_ID0
    ,Credit_EFFECTIVE_DATE
    ,Credit_MOD_DATE
    ,Credit_Type
    ,Credit_Reason_ID
    ,Credit_Version_ID
    ,Credit_Reason
    ,TOTAL
    ,tblload_dtt
    ,ITEM_NO
    ,CURRENCY_ID
	,EVENT_POID_TYPE
	,SERVICE_TYPE
	,QUANTITY

FROM
(SELECT
   A.ACCOUNT_NO                                                    AS BRM_Account_No,
   SUBSTR(A.ACCOUNT_NO, STRPOS (A.ACCOUNT_NO,'-') + 1, 60)   AS Account,
   A.NAME                                                        AS AccountName,
   ebi.GL_ID                                                         AS EBI_GL_ID,
   A.GL_Segment,
   E.POID_ID0                                                        AS Event_POID_ID0,                                        
   I.POID_ID0                                                        AS Item_POID_ID0,
   I.POID_TYPE                                                        AS Item_POID_TYPE,
   I.BILL_OBJ_ID0,
   cast(TIMESTAMP_ADD(TIMESTAMP("1970-01-01 00:00:00"), INTERVAL cast(i.EFFECTIVE_T  as int64) second)  as datetime) AS Credit_EFFECTIVE_DATE,
   cast(TIMESTAMP_ADD(TIMESTAMP("1970-01-01 00:00:00"), INTERVAL cast(i.MOD_T  as int64) second)  as datetime) AS Credit_MOD_DATE,
   S.STRING                                                        AS Credit_Type,
   EBM.REASON_ID                                                    AS Credit_Reason_ID,
   REASON_DOMAIN_ID                                                AS Credit_Version_ID,
   E.DESCR                                                        AS Credit_Reason,
   IFNULL(CAST(EBI.AMOUNT AS NUMERIC), 0)                                AS Total,
   current_datetime()                                                        AS tblload_dtt,
   I.ITEM_NO,
   EBI.RESOURCE_ID                                                    AS Currency_ID,            -- new field added 01.13.16jcm
	E.POID_TYPE AS Event_POID_Type,        -- New Field 16-10-2019 upon discussion with Uday
	IFNULL(I.SERVICE_OBJ_TYPE,'N/A') AS Service_Type,		-- New Field 16-10-2019 upon discussion with Uday
	EBI.QUANTITY AS Quantity		-- New Field 16-10-2019 upon discussion with Uday
FROM
   `rax-landing-qa`.brm_ods.item_t AS I 
INNER JOIN
    `rax-landing-qa`.brm_ods.account_t  A 
ON I.ACCOUNT_OBJ_ID0 = A.POID_ID0
AND I.BILL_OBJ_ID0 = 0
INNER JOIN
    `rax-landing-qa`.brm_ods.event_t E 
ON I.POID_ID0 = E.ITEM_OBJ_ID0
INNER JOIN
    `rax-landing-qa`.brm_ods.event_bal_impacts_t  EBI 
ON E.POID_ID0 = EBI.OBJ_ID0
LEFT OUTER JOIN
    `rax-landing-qa`.brm_ods.event_billing_misc_t EBM 
ON E.POID_ID0 = EBM.OBJ_ID0
LEFT OUTER JOIN
    `rax-landing-qa`.brm_ods.strings_t  S 
ON EBM.REASON_ID = S.STRING_ID
AND EBM.REASON_DOMAIN_ID = S.VERSION
WHERE
   lower(I.POID_Type)='/item/adjustment'
AND lower(E.POID_TYPE) like ('%adjustment%')
AND I.MOD_T>=GETDATE_UNIX
AND (E.SERVICE_OBJ_TYPE IS NULL)
AND (IFNULL(CAST(EBI.AMOUNT AS numeric), 0) < 0)
AND lower(a.gl_segment) like  ('%cloud%')
);

END;
