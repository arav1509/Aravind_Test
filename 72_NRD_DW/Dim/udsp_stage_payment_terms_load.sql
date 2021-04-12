CREATE OR REPLACE PROCEDURE `rax-staging-dev.stage_two_dw.udsp_stage_payment_terms_load`()
BEGIN
 
  delete from `rax-staging-dev.stage_two_dw.stage_payment_terms` TARGET
where  TARGET.payment_term_nk in
(
select cast(SOURCE.Rec_Id as INT64)
from `rax-staging-dev.stage_one.raw_payment_terms` SOURCE
);
 
 
insert into  `rax-staging-dev.stage_two_dw.stage_payment_terms`(Payment_Term_NK,Payment_Term_Name,
payment_term_description,Chk_Sum_Md5,Source_System_Name,Record_Created_By,Record_Created_Datetime,Record_Updated_By,Record_Updated_Datetime)
 SELECT 
Payment_Term_NK,
Payment_Term_Name,
Payment_Term_Desc,
TO_BASE64(MD5(CONCAT(Payment_Term_NK, '|' ,Payment_Term_Name,'|' , PAYMENT_TERM_DESC ,'|' )))  AS Chk_Sum_Md5,
	   Source_System_Name,
	    'SSIS_Stage_Payment_Terms' AS Record_Created_By
	   ,cast(SRC.Record_Created_Datetime as datetime) as Record_Created_Datetime
	   ,'SSIS_Stage_Payment_Terms' AS Record_Updated_By
	   ,SRC.Record_Created_Datetime  AS Record_Updated_Datetime
	    from 
(
SELECT  
	cast(Rec_ID as INT64) as Payment_Term_NK,
	COALESCE(Payment_Term_Name,'UNKNOWN') as Payment_Term_Name,
	COALESCE(Payment_Term_Desc,'UNKNOWN') as Payment_Term_Desc,
	CURRENT_DATETIME() AS Record_Created_Datetime,
	Source_System_Name
 FROM stage_one.raw_payment_terms)SRC;
 
 END;
