CREATE OR REPLACE PROCEDURE `rax-staging-dev.stage_one.udsp_etl_raw_brm_dedicated_invoice_line_item_audit`()
BEGIN


--SELECT * FROM #Negative_Diff  order by Diff_into_pos DESC
INSERT INTO
    stage_one.raw_brm_audit_dedicated_invoice_excludes
SELECT 
    BILL_NO  
FROM 
   (
		SELECT Diff_Current_Total*-1 as Diff_into_pos,* -- #Negative_Diff
		FROM 
			(
					SELECT DISTINCT  --#BRM_Dedicated_Invoice_Audit_stage
					BILL_NO,
					BILL_END_DATE,
					Source_Total,
					Line_Item_Total,
					Diff_Current_Total  
				FROM
					(
							SELECT DISTINCT  -- #Raw_BRM_Dedicated_Invoice_line_Item_Audit
								BILL_NO,
								BILL_END_DATE,
								TOTAL_DUE															   AS Source_Total,
								IfNULL(B.Line_Item_Total,0)											   AS Line_Item_Total,
								trunc(CAST(TOTAL_DUE-IfNULL(B.Line_Item_Total,0) as numeric),2)			   AS Diff_Current_Total
							FROM
								(
										SELECT DISTINCT  --#BRM_Source_Total 
											BILL_NO,   
											BILL_END_DATE,
											SUM(TOTAL_DUE)					    AS TOTAL_DUE  
										FROM
										   (
												SELECT DISTINCT   --#BRM_Source_Total_Stage
													ACCOUNT_ID,
													COMPANY_NAME,
													BILL_POID_ID0					    AS bill_poid,
													BILL_NO,
													BILL_END_DATE,
													BILL_MOD_DATE,
													trunc(CAST(CURRENT_TOTAL as NUMERIC),2)    AS TOTAL_DUE
												FROM
													stage_one.raw_brm_dedicated_invoice_aggregate_total A
												WHERE
													`Exclude`=0
												AND  (CURRENT_TOTAL)<>0  
												AND upper(BILL_NO) not like '%EBS%'
										   ) A --#BRM_Source_Total_Stage A  
										GROUP BY
											 BILL_NO,
											 BILL_END_DATE
								) A --#BRM_Source_Total A
							LEFT OUTER JOIN
								(
									SELECT   --#BRM_Invoice_Line_Item
										Trx_Number				    AS Invoice,
									   trunc(CAST(SUM(TOTAL) as numeric),2)  AS Line_Item_Total
									FROM
										stage_two_dw.stage_dedicated_inv_event_detail 
									WHERE 1=1 
									and 	Invoice_Date >= DATE_TRUNC(DATE_ADD(CURRENT_DATE(), INTERVAL -4 YEAR), year)
									AND	Invoice_Date <= CURRENT_DATE()
									GROUP BY
										Trx_Number
								) B--#BRM_Invoice_Line_Item B
							ON A.BILL_NO=B.Invoice
					) A --#Raw_BRM_Dedicated_Invoice_line_Item_Audit A
				WHERE
				  (Source_Total<> Line_Item_Total)
				AND BILL_NO NOT IN(SELECT DISTINCT BILL_NO FROM  stage_one.raw_brm_audit_dedicated_invoice_excludes)
			)--#BRM_Dedicated_Invoice_Audit_stage 
		WHERE   
			Diff_Current_Total <0  
		ORDER BY
			 Diff_Current_Total 
	)--   #Negative_Diff  
WHERE     Diff_into_pos < 1;
-------------------------------------------------------------------------------------------     

--SELECT * FROM #Positve_Diff  order by Diff_Current_Total DESC
INSERT INTO stage_one.raw_brm_audit_dedicated_invoice_excludes
SELECT 
    BILL_NO 
FROM 
    (
		SELECT     distinct * -- #Positve_Diff
		FROM 
			(
					SELECT DISTINCT  --#BRM_Dedicated_Invoice_Audit_stage
							BILL_NO,
							BILL_END_DATE,
							Source_Total,
							Line_Item_Total,
							Diff_Current_Total  
						FROM
							(
									SELECT DISTINCT  -- #Raw_BRM_Dedicated_Invoice_line_Item_Audit
										BILL_NO,
										BILL_END_DATE,
										TOTAL_DUE															   AS Source_Total,
										IfNULL(B.Line_Item_Total,0)											   AS Line_Item_Total,
										trunc(CAST(TOTAL_DUE-IfNULL(B.Line_Item_Total,0) as numeric),2)			   AS Diff_Current_Total
									FROM
										(
												SELECT DISTINCT  --#BRM_Source_Total 
													BILL_NO,   
													BILL_END_DATE,
													SUM(TOTAL_DUE)					    AS TOTAL_DUE  
												FROM
												   (
														SELECT DISTINCT   --#BRM_Source_Total_Stage
															ACCOUNT_ID,
															COMPANY_NAME,
															BILL_POID_ID0					    AS bill_poid,
															BILL_NO,
															BILL_END_DATE,
															BILL_MOD_DATE,
															trunc(CAST(CURRENT_TOTAL as NUMERIC),2)    AS TOTAL_DUE
														FROM
															stage_one.raw_brm_dedicated_invoice_aggregate_total A
														WHERE
															`Exclude`=0
														AND  (CURRENT_TOTAL)<>0  
														AND upper(BILL_NO) not like '%EBS%'
												   ) A --#BRM_Source_Total_Stage A  
												GROUP BY
													 BILL_NO,
													 BILL_END_DATE
										) A --#BRM_Source_Total A
									LEFT OUTER JOIN
										(
											SELECT   --#BRM_Invoice_Line_Item
												Trx_Number				    AS Invoice,
											   trunc(CAST(SUM(TOTAL) as numeric),2)  AS Line_Item_Total
											FROM
												stage_two_dw.stage_dedicated_inv_event_detail 
											WHERE 1=1 
											and 	Invoice_Date >= DATE_TRUNC(DATE_ADD(CURRENT_DATE(), INTERVAL -4 YEAR), year)
											AND	Invoice_Date <= CURRENT_DATE()
											GROUP BY
												Trx_Number
										) B--#BRM_Invoice_Line_Item B
									ON A.BILL_NO=B.Invoice
							) A --#Raw_BRM_Dedicated_Invoice_line_Item_Audit A
						WHERE
						  (Source_Total<> Line_Item_Total)
						AND BILL_NO NOT IN(SELECT DISTINCT BILL_NO FROM  stage_one.raw_brm_audit_dedicated_invoice_excludes)
					) --#BRM_Dedicated_Invoice_Audit_stage 
		WHERE   
			Diff_Current_Total > 0
		
	)--#Positve_Diff  
WHERE
    Diff_Current_Total >0
AND  Diff_Current_Total <1
ORDER BY			 Diff_Current_Total;
-------------------------------------------------------------------------------------------

create or replace table stage_one.raw_brm_dedicated_invoice_line_item_audit
as 
SELECT DISTINCT 
    BILL_NO,
    BILL_END_DATE,
    Source_Total,
    Line_Item_Total,
    Diff_Current_Total,
    CURRENT_DATE()						    AS Refresh_Date

FROM
    (
					SELECT DISTINCT  --#BRM_Dedicated_Invoice_Audit_stage
							BILL_NO,
							BILL_END_DATE,
							Source_Total,
							Line_Item_Total,
							Diff_Current_Total  
						FROM
							(
									SELECT DISTINCT  -- #Raw_BRM_Dedicated_Invoice_line_Item_Audit
										BILL_NO,
										BILL_END_DATE,
										TOTAL_DUE															   AS Source_Total,
										IfNULL(B.Line_Item_Total,0)											   AS Line_Item_Total,
										trunc(CAST(TOTAL_DUE-IfNULL(B.Line_Item_Total,0) as numeric),2)			   AS Diff_Current_Total
									FROM
										(
												SELECT DISTINCT  --#BRM_Source_Total 
													BILL_NO,   
													BILL_END_DATE,
													SUM(TOTAL_DUE)					    AS TOTAL_DUE  
												FROM
												   (
														SELECT DISTINCT   --#BRM_Source_Total_Stage
															ACCOUNT_ID,
															COMPANY_NAME,
															BILL_POID_ID0					    AS bill_poid,
															BILL_NO,
															BILL_END_DATE,
															BILL_MOD_DATE,
															trunc(CAST(CURRENT_TOTAL as NUMERIC),2)    AS TOTAL_DUE
														FROM
															stage_one.raw_brm_dedicated_invoice_aggregate_total A
														WHERE
															`Exclude`=0
														AND  (CURRENT_TOTAL)<>0  
														AND upper(BILL_NO) not like '%EBS%'
												   ) A --#BRM_Source_Total_Stage A  
												GROUP BY
													 BILL_NO,
													 BILL_END_DATE
										) A --#BRM_Source_Total A
									LEFT OUTER JOIN
										(
											SELECT   --#BRM_Invoice_Line_Item
												Trx_Number				    AS Invoice,
											   trunc(CAST(SUM(TOTAL) as numeric),2)  AS Line_Item_Total
											FROM
												stage_two_dw.stage_dedicated_inv_event_detail 
											WHERE 1=1 
											and 	Invoice_Date >= DATE_TRUNC(DATE_ADD(CURRENT_DATE(), INTERVAL -4 YEAR), year)
											AND	Invoice_Date <= CURRENT_DATE()
											GROUP BY
												Trx_Number
										) B--#BRM_Invoice_Line_Item B
									ON A.BILL_NO=B.Invoice
							) A --#Raw_BRM_Dedicated_Invoice_line_Item_Audit A
						WHERE
						  (Source_Total<> Line_Item_Total)
						AND BILL_NO NOT IN(SELECT DISTINCT BILL_NO FROM  stage_one.raw_brm_audit_dedicated_invoice_excludes)
	)--#BRM_Dedicated_Invoice_Audit_stage
WHERE
   Source_Total <> Line_Item_Total 
AND BILL_NO NOT IN(SELECT DISTINCT BILL_NO FROM  stage_one.raw_brm_audit_dedicated_invoice_excludes);

END;
