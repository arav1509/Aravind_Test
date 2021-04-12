CREATE OR REPLACE PROCEDURE `rax-staging-dev.stage_two_dw.udsp_etl_stage_update_dedicated_invoice_null_bill_no`()
BEGIN
	
------------------------------------------------------------------------------------------------------------
DECLARE CurrentDate datetime;

DECLARE subject_failure string;
DECLARE body string;

SET CurrentDate=CURRENT_DATETIME();
-------------------------------------------------------------------------------------------------------------

UPDATE stage_two_dw.stage_dedicated_inv_event_detail  A
SET
    A.Bill_NO=B.Bill_NO,
    A.Trx_Number=IFNULL(B.Bill_NO,'Unknown'),--B.Bill_NO,
    A.Invoice_Date=B.Bill_End_Date,
    A.Time_Month_Key=cast(B.Time_Month_Key as int64),
    A.Bill_Start_Date =B.BILL_START_DATE,
    A.Bill_End_Date=B.BILL_END_DATE,
    A.Bill_Mod_Date=B.BILL_MOD_DATE,
    A.Trx_Line_NK = CONCAT(CAST(master_unique_id as STRING),'--',CAST(gl_Account as STRING),'--',CAST(B.Time_Month_Key as STRING),'--',CAST(IFNULL(Server,'1') as STRING),'--',(case when Trx_Term = '0' then CAST(Trx_Qty AS STRING) else CAST(trx_term as STRING) end)),
	A.Etldtt = CurrentDate
FROM stage_one.raw_brm_dedicated_invoice_aggregate_total B 
WHERE A.ITEM_Bill_Obj_Id0=B.BILL_POID_ID0
and 
  (A.Bill_NO<>B.Bill_NO
OR A.Trx_Number<>B.Bill_NO
OR A.Time_Month_Key<>cast(B.Time_Month_Key as int64)
OR IFNULL(A.Invoice_Date,'1900-01-01')<>IFNULL(B.Bill_End_Date,'1900-01-01')
OR IFNULL(A.Bill_Start_Date,'1900-01-01')<>IFNULL(B.BILL_START_DATE,'1900-01-01')
OR IFNULL(A.Bill_End_Date,'1900-01-01')<>IFNULL(B.BILL_END_DATE,'1900-01-01')
OR IFNULL(A.Bill_Mod_Date,'1900-01-01')<>IFNULL(B.BILL_MOD_DATE,'1900-01-01')
);
-------------------------------------------------------------------------------------------------------------
UPDATE  stage_two_dw.stage_dedicated_inv_event_detail  A 
SET
    A.Bill_NO=B.Bill_NO,
    A.Trx_Number=B.Bill_NO,
    A.Invoice_Date=B.Bill_End_Date,
    A.Time_Month_Key=B.Time_Month_Key,
    A.Bill_Start_Date =B.BILL_START_DATE,
    A.Bill_End_Date=B.BILL_END_DATE,
    A.Bill_Mod_Date=B.BILL_MOD_DATE,
    A.Trx_Line_NK = CONCAT(CAST(master_unique_id as STRING),'--',CAST(gl_Account as STRING),'--',CAST(B.Time_Month_Key as STRING),'--',CAST(IFNULL(Server,'1') as STRING),'--',(case when Trx_Term = '0' then CAST(Trx_Qty as STRING) else CAST(trx_term as STRING) end)),
	A.Etldtt = CurrentDate
FROM
    stage_one.raw_brm_invoice_aggregate_total B 
WHERE A.ITEM_Bill_Obj_Id0=B.BILL_POID_ID0
AND
  (A.Bill_NO<>B.Bill_NO
OR A.Trx_Number<>B.Bill_NO
OR A.Time_Month_Key<>B.Time_Month_Key
OR IFNULL(A.Invoice_Date,'1900-01-01')<>IFNULL(B.Bill_End_Date,'1900-01-01')
OR IFNULL(A.Bill_Start_Date,'1900-01-01')<>IFNULL(B.BILL_START_DATE,'1900-01-01')
OR IFNULL(A.Bill_End_Date,'1900-01-01')<>IFNULL(B.BILL_END_DATE,'1900-01-01')
OR IFNULL(A.Bill_Mod_Date,'1900-01-01')<>IFNULL(B.BILL_MOD_DATE,'1900-01-01')
);
-------------------------------------------------------------------------------------------------------------
--EXEC msdb..Usp_send_cdosysmail 'no_replyrackspace.com','TES_ETL_Supportrackspace.com','NRD Dedicated Invoice_Date Update Job Success',''
/*
BEGIN CATCH

	set subject_failure = 'NRD Dedicated_Invoice_Null_Bill_No post update Failure Notification';
	DECLARE body nvarchr(max) = 'Data Transformation Failed during Fact Table Load' 
	+ chr(10) + chr(13) + 'Error Number:  ' + CAST(ERROR_NUMBER() AS string)
	+ chr(10) + chr(13) + 'Error Severity:  ' + CAST(ERROR_SEVERITY() AS string)
	+ chr(10) + chr(13) + 'Error State:  ' + CAST(ERROR_STATE() AS string)
	+ chr(10) + chr(13) + 'Error Procedure:  ' + CAST(ERROR_PROCEDURE() AS string)
	+ chr(10) + chr(13) + 'Error Line:  ' + CAST(ERROR_LINE() AS string)
	+ chr(10) + chr(13) + 'Error Message: ' + ERROR_MESSAGE()
	+ chr(10) + chr(13) + chr(10) + chr(13) + chr(10) + chr(13) + chr(10) + chr(13) + 'This is a system generated mail. DO NOT REPLY  ';
	DECLARE to nvarchr(max) = 'TES_ETL_Supportrackspace.com';
	DECLARE profile_name sysname = 'Jobs';
	EXEC msdb.dbo.sp_send_dbmail profile_name = profile_name,
	recipients = to, subject = subject_failure, body = body;

    
END CATCH
*/
END;
