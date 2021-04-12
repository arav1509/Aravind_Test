CREATE OR REPLACE PROCEDURE `rax-abo-72-dev`.sales.udsp_etl_partner_opportunity_audit()
BEGIN
IF EXTRACT(DAY FROM CURRENT_DATE()) = 5
THEN

-------------------------------------------------------------------------------------------------------------------
CREATE OR REPLACE TABLE `rax-abo-72-dev`.sales.partner_opportunity_audit AS
SELECT
	O.Opportunity_ID,
	O.ID as Opportunity_Long_ID,
	O.Closedate as Close_Date,
	P.Partner_Company as Partner_Long_ID,	
	P.Commission_Role,
	P.Rolex as Partner_Role,
	A.Namex as Partner_Account_Name,
	A.Contract_Signed_Date,
	A.Contract_Type,
	O.Booking,
	O.Amount,
	O.Approval_Amount
FROM
	`rax-landing-dev`.salesforce_ods.qopportunity O 
JOIN
	`rax-landing-dev`.salesforce_ods.qpartner_role P 
ON O.id = P.Opportunity
JOIN
	`rax-landing-dev`.salesforce_ods.qaccount A 
ON P.Partner_Company = A.id
WHERE
	`rax-abo-72-dev`.bq_functions.udf_yearmonth_nohyphen(CloseDate) = `rax-abo-72-dev`.bq_functions.udf_yearmonth_nohyphen(date_trunc(current_date(), year)-1)
AND lower(O.Stagename) = 'closed won'
AND O.Delete_Flag <> 'Y'
AND O.Approval_Amount > 25000;
ELSE 
 SELECT 1;
end if;
end;
