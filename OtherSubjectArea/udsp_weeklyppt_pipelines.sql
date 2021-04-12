CREATE OR REPLACE PROCEDURE `rax-abo-72-dev`.report_tables.udsp_weeklyppt_pipelines()
begin
/*********************************
** Put together hierarchy Table **
*********************************/
DECLARE StartDate  DATE;
DECLARE EndDate  DATE;
-----------------------------------------------------------------------------------------------------------------
SET StartDate = date_trunc(current_date(),month);
SET EndDate = last_day(date_add(current_date(), interval 1 month));
-----------------------------------------------------------------------------------------------------------------
/***********************
** get conversionRate **
************************/
create or replace temp table exchange as
SELECT--INTO       #exchange
    startdate,
    nextstartdate,
    isocode,
    conversionrate

FROM   
    `rax-landing-qa`.salesforce_ods.qdatedconversionrate
WHERE  
    nextstartdate >= current_date()
AND startdate <= current_date()
AND delete_Flag = 'N';

--add usd to simplify the conversion later on
INSERT INTO exchange
SELECT current_date(),
       current_date(),
       'USD',
       1;

create or replace table  `rax-abo-72-dev`.report_tables.weeklyppt_pipelines as
SELECT current_date() as Load_Date,
       CASE
         WHEN forecastcategoryname = 'closed' THEN 'MTD'
         ELSE 'Pipeline'
       END                                        AS Report,
       FORMAT_DATE('%B', current_date()) AS Month,
       CASE
         WHEN o1.booking < 0 THEN 'Zero'
		 WHEN o1.booking <= 500 THEN 'One'
		 WHEN o1.booking <= 3000 THEN 'Two'
		 WHEN o1.booking <= 10000 THEN 'Three'
		 WHEN o1.booking <= 30000 THEN 'Four'
		 WHEN o1.booking <= 100000 THEN 'Five'
		 else 'Six'
       END AS Revenue_Bucket,
       Sum(o1.booking / x.conversionrate) AS booking_Converted,
       Count(o1.id) AS `count`
FROM   
    `rax-landing-qa`.salesforce_ods.qopportunity o1 
INNER JOIN
    (SELECT DISTINCT Load_Date_Time_Month_Key, Account_Owner_ID,Account_Owner_Role  
		FROM `rax-abo-72-dev`.sales.eom_account_owner_snapshot  
		WHERE Account_Owner_ISACTIVE ='TRUE') User1
ON o1.ownerid=User1.Account_Owner_ID
AND  `rax-abo-72-dev`.bq_functions.udf_yearmonth_nohyphen(closedate)=User1.Load_Date_Time_Month_Key
INNER JOIN
    `rax-abo-72-dev`.sales.dim_sales_us_team_hierarchy  Hier 
ON User1.Account_Owner_Role=Hier.Team
AND Load_Date_Time_Month_Key=Time_Month_Key
--AND o1.Split_Category NOT LIKE '%secondary'
AND Is_Active=1
LEFT JOIN 
    exchange x
ON o1.CURRENCYISOCODE = x.ISOCODE
WHERE  
    CloseDate BETWEEN StartDate AND endDate
AND lower(forecastcategoryname) NOT IN ( 'omitted' )
AND o1.booking > 0
AND lower(Account_Owner_Role) NOT LIKE '%sharepoint%'
AND lower(Account_Owner_Role) NOT LIKE '%deal center%'
/*AND ( o1.FINAL_OPPORTUNITY_TYPE NOT IN ( 'cloud', 'Mail', 'Managed Cloud' )
      OR o1.FINAL_OPPORTUNITY_TYPE IS NULL )
	  */
AND ( stagename LIKE 'Stage 1-5%'
      OR lower(forecastcategoryname) = 'closed' )
GROUP  by forecastcategoryname, o1.booking;     

                               

end
