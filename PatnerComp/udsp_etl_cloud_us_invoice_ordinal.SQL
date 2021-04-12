CREATE OR REPLACE PROCEDURE `rax-abo-72-dev`.slicehost.udsp_etl_cloud_us_invoice_ordinal()
BEGIN

create or replace table `rax-abo-72-dev`.slicehost.cloud_us_invoiced_w_ordinal AS
SELECT  	
	A.Account_Key,
	A.Account,
	TotalPrice,
	A.Time_Month_Key,
	Invoice_Ordinal
FROM
	(
		SELECT DISTINCT  --INTO	#Invoice_Data
			Cloud_Account_Key						AS Account_Key,
			Account,
			SUM(Item_Price)							AS TotalPrice,
			Time_Month_Key
		FROM
			`rax-abo-72-dev`.slicehost.cloud_invoice_detail A
		WHERE
			LOWER(Product_Group) <> 'taxes'
		GROUP BY
			Cloud_Account_Key,
			Account,
			Time_Month_Key
	) A --#Invoice_Data A
INNER JOIN
	(
		SELECT DISTINCT --INTO	#Cloud_US_Invoice_Ordinal
			Account_Key,
			Account,
			Time_Month_Key,
			ROW_NUMBER() OVER(PARTITION BY Account ORDER BY Time_Month_Key ASC)	AS Invoice_Ordinal
		FROM
			(
				SELECT DISTINCT  --INTO	#Invoice_Data
					Cloud_Account_Key						AS Account_Key,
					Account,
					SUM(Item_Price)							AS TotalPrice,
					Time_Month_Key
				FROM
					`rax-abo-72-dev`.slicehost.cloud_invoice_detail A
				WHERE
					LOWER(Product_Group) <> 'taxes'
				GROUP BY
					Cloud_Account_Key,
					Account,
					Time_Month_Key
			) A
	) B --#Cloud_US_Invoice_Ordinal B
ON A.Account=B.Account
AND A.Time_Month_Key=B.Time_Month_Key
ORDER BY
	Account,
	Invoice_Ordinal
;

END;
