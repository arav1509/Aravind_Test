CREATE OR REPLACE PROCEDURE `rax-abo-72-dev`.report_tables.udsp_weeklyppt_growth()
BEGIN


DECLARE ThisMonth int64;
DECLARE NextMonth int64;

SET thisMonth = `rax-abo-72-dev`.bq_functions.udf_yearmonth_nohyphen(current_date());
SET nextMonth = case when mod(thisMonth, 100) = 12 then thisMonth + 89 else thisMonth + 1 end;
/***********************
** get conversionRate **
************************/
create or replace temp table exchange as 
SELECT startdate,--INTO       #exchange
       nextstartdate,
       isocode,
       conversionrate

FROM  
    `rax-landing-qa`.salesforce_ods.qdatedconversionrate
WHERE  
    nextstartdate >= current_date()
AND startdate	 <= current_date()
AND delete_Flag = 'N';

--add usd to simplify the conversion later on
INSERT INTO  exchange
SELECT current_date(),
       current_date(),
       'USD',
       1;
create or replace temp table Commit as
SELECT  --INTO      #Commit
     FORMAT_DATE('%B', current_date()) AS Month,
    forecastCategoryName,
    o1.ownerid,
    o1.id,
    o1.LastModifiedDate,
    concat(Cast(COALESCE(a.namex, 'NoName') AS string), ' - ' , Segment , ' - ' , substr(COALESCE(StageName, 'NoStage'), 0, 8))  AS Customer_Segment_Stage,
    trunc(Cast(o1.APPROVAL_AMOUNT / x.conversionrate AS numeric ),2)			    AS USD
FROM   
    `rax-landing-dev`.salesforce_ods.qopportunity o1 
INNER JOIN
    (SELECT DISTINCT Load_Date_Time_Month_Key, Account_Owner_ID,Account_Owner_Role  
		FROM `rax-abo-72-dev`.sales.eom_account_owner_snapshot  
		WHERE upper(Account_Owner_ISACTIVE) ='TRUE' and Load_Date_Time_Month_Key=ThisMonth
	) User1
ON o1.ownerid=User1.Account_Owner_ID
AND  `rax-abo-72-dev`.bq_functions.udf_yearmonth_nohyphen(closedate)=User1.Load_Date_Time_Month_Key
INNER JOIN
    `rax-abo-72-dev`.sales.dim_sales_us_team_hierarchy  Hier 
ON User1.Account_Owner_Role=Hier.Team
AND Load_Date_Time_Month_Key=Time_Month_Key
--AND lower(o1.Split_Category) NOT LIKE '%secondary' ---Split_Category not found
AND Is_Active=1
LEFT JOIN 
    exchange x
ON o1.CURRENCYISOCODE = x.ISOCODE
INNER JOIN 
    `rax-landing-qa`.salesforce_ods.qaccount a 
ON o1.accountid = a.id
WHERE  
   `rax-abo-72-dev`.bq_functions.udf_yearmonth_nohyphen(closedate) = ThisMonth
AND o1.APPROVAL_AMOUNT * x.conversionrate >= 10000
AND lower(forecastCategoryName) NOT In ( 'omitted' )
AND lower(Account_Owner_Role) NOT LIKe '%sharepoint%'
AND lower(Account_Owner_Role) NOT LIKe '%deal center%'
/*AND ( lower(o1.FINAL_OPPORTUNITY_TYPE) not in ( 'cloud', 'mail', 'managed cloud' )
OR o1.FINAL_OPPORTUNITY_TYPE IS NULL )
*/
AND lower(forecastCategoryName) = 'commit'
ORDER  BY ( o1.APPROVAL_AMOUNT * x.conversionrate ) DESC
limit 25
;       

--Top 25 this month's Best Case
create or replace temp table BestCase as
SELECT--INTO       #BestCase
    FORMAT_DATE('%B', current_date()) AS Month,
    forecastCategoryName,
    o1.id,
    o1.LastModifiedDate,
    concat(Cast(COALESCE(a.namex, 'NoName') AS string) , ' - ' , Segment , ' - ' , substr(COALESCE(StageName, 'NoStage'), 0, 8))   AS Customer_Segment_Stage,
    trunc(Cast(o1.APPROVAL_AMOUNT * x.conversionrate AS numeric), 2)			    AS USD
FROM   
    `rax-landing-qa`.salesforce_ods.qopportunity o1 
INNER JOIN
    (	SELECT DISTINCT Load_Date_Time_Month_Key, Account_Owner_ID,Account_Owner_Role  
		FROM  `rax-abo-72-dev`.sales.eom_account_owner_snapshot  
		WHERE upper(Account_Owner_ISACTIVE) ='TRUE' and Load_Date_Time_Month_Key=ThisMonth
	) User1
ON o1.ownerid=User1.Account_Owner_ID
AND  `rax-abo-72-dev`.bq_functions.udf_yearmonth_nohyphen(closedate)=User1.Load_Date_Time_Month_Key
INNER JOIN
     `rax-abo-72-dev`.sales.dim_sales_us_team_hierarchy  Hier 
ON User1.Account_Owner_Role=Hier.Team
AND Load_Date_Time_Month_Key=Time_Month_Key
--AND lower(o1.Split_Category) NOT LIKE '%secondary' ---Split_Category not found
AND Is_Active=1
LEFT JOIN 
    exchange x
ON o1.CURRENCYISOCODE = x.ISOCODE
INNER JOIN 
   `rax-landing-qa`.salesforce_ods.qaccount a 
ON o1.accountid = a.id
WHERE  
    `rax-abo-72-dev`.bq_functions.udf_yearmonth_nohyphen(closedate) = ThisMonth
AND o1.APPROVAL_AMOUNT * x.conversionrate >= 10000
AND lower(forecastCategoryName) NOT IN ( 'omitted' )
AND lower(Account_Owner_Role) Not like '%sharepoint%'
AND lower(Account_Owner_Role) Not like '%deal center%'
--AND ( lower(o1.FINAL_OPPORTUNITY_TYPE) not in ( 'cloud', 'mail', 'managed cloud' )
--  OR  o1.FINAL_OPPORTUNITY_TYPE IS NULL )
AND lower(forecastCategoryName) = 'best case'
ORDER  BY ( o1.APPROVAL_AMOUNT * x.conversionrate ) DESC
limit 25;

--Top 25 this month's Pipeline
create or replace temp table pipeline as
SELECT --INTO    #pipeline
    FORMAT_DATE('%B', current_date()) AS Month,
    forecastCategoryName,
    o1.id,
    o1.LastModifiedDate,
    concat(Cast(COALESCE(a.namex, 'NoName') AS string)  , ' - ' , Segment , ' - ', substr(COALESCE(StageName, 'NoStage'), 0, 8))    AS Customer_Segment_Stage,
    trunc(Cast(o1.APPROVAL_AMOUNT * x.conversionrate AS numeric ), 2)			    AS USD
FROM   
    `rax-landing-qa`.salesforce_ods.qopportunity o1 
INNER JOIN
    (
	SELECT DISTINCT Load_Date_Time_Month_Key, Account_Owner_ID,Account_Owner_Role  
	FROM `rax-abo-72-dev`.sales.eom_account_owner_snapshot  
	WHERE upper(Account_Owner_ISACTIVE) ='TRUE' and Load_Date_Time_Month_Key=ThisMonth
	) User1
ON  o1.ownerid=User1.Account_Owner_ID
AND `rax-abo-72-dev`.bq_functions.udf_yearmonth_nohyphen(closedate)=User1.Load_Date_Time_Month_Key
INNER JOIN
    `rax-abo-72-dev`.sales.dim_sales_us_team_hierarchy  Hier 
ON User1.Account_Owner_Role=Hier.Team
AND Load_Date_Time_Month_Key=Time_Month_Key
--AND lower(o1.Split_Category) NOT LIKE '%secondary'---Split_Category not found
AND Is_Active=1
LEFT JOIN 
    exchange x
ON o1.CURRENCYISOCODE = x.ISOCODE
INNER JOIN 
    `rax-landing-qa`.salesforce_ods.qaccount a 
ON o1.accountid = a.id
WHERE  
    `rax-abo-72-dev`.bq_functions.udf_yearmonth_nohyphen(closedate) = ThisMonth
    and o1.approval_amount * x.conversionrate >= 10000
    and lower(forecastcategoryname) not in ( 'omitted' )
    and lower(account_owner_role) not like '%sharepoint%'
    and lower(account_owner_role) not like '%deal center%'
    --and ( lower(o1.final_opportunity_type) not in ( 'cloud', 'mail', 'managed cloud' )
	--or o1.final_opportunity_type is null )
    and lower(forecastcategoryname) = 'pipeline'
ORDER  BY ( o1.APPROVAL_AMOUNT * x.conversionrate ) DESC
limit 25;


--Top 25 Next month's Commit
create or replace temp table Commit2 as
SELECT --INTO       #Commit2
	 FORMAT_DATE('%B', Date_add(current_date(), interval 1 month)) AS Month,
    forecastCategoryName,
    o1.id,
    o1.LastModifiedDate,
    concat(Cast(COALESCE(a.namex, 'NoName') AS string) , ' - ' , Segment , ' - ' , substr(COALESCE(StageName, 'NoStage'), 0, 8))  AS Customer_Segment_Stage,
    trunc(Cast(o1.APPROVAL_AMOUNT * x.conversionrate AS numeric), 2)			    AS USD
FROM   
    `rax-landing-qa`.salesforce_ods.qopportunity o1 
INNER JOIN
    (SELECT DISTINCT Load_Date_Time_Month_Key, Account_Owner_ID,Account_Owner_Role  
	FROM `rax-abo-72-dev`.sales.eom_account_owner_snapshot  
	WHERE upper(Account_Owner_ISACTIVE) ='TRUE' and Load_Date_Time_Month_Key=ThisMonth
	) User1
ON  o1.ownerid=User1.Account_Owner_ID
AND `rax-abo-72-dev`.bq_functions.udf_yearmonth_nohyphen(closedate)=User1.Load_Date_Time_Month_Key
INNER JOIN
    `rax-abo-72-dev`.sales.dim_sales_us_team_hierarchy  Hier 
ON User1.Account_Owner_Role=Hier.Team
AND Load_Date_Time_Month_Key=Time_Month_Key
--AND lower(o1.Split_Category) NOT LIKE '%secondary'
AND Is_Active=1
LEFT JOIN 
    exchange x
ON o1.CURRENCYISOCODE = x.ISOCODE
INNER JOIN 
    `rax-landing-qa`.salesforce_ods.qaccount a 
ON o1.accountid = a.id
WHERE  
    `rax-abo-72-dev`.bq_functions.udf_yearmonth_nohyphen(closedate) = NextMonth
    AND o1.APPROVAL_AMOUNT * x.conversionrate >= 10000
    and lower(forecastcategoryname) not in ( 'omitted' )
    and lower(account_owner_role) not like '%sharepoint%'
    and lower(account_owner_role) not like '%deal center%'
   -- and ( lower(o1.final_opportunity_type) not in ( 'cloud', 'mail', 'managed cloud' )
    --or o1.final_opportunity_type is null )
    and lower(forecastcategoryname) = 'commit'
ORDER  BY 
    ( o1.APPROVAL_AMOUNT * x.conversionrate ) DESC
	limit 25;

--Top 25 Next month's Best Case
create or replace temp table BestCase2 as
SELECT --INTO       #BestCase2
    FORMAT_DATE('%B', Date_add(current_date(), interval 1 month)) AS Month,
    forecastCategoryName,
    o1.id,
    o1.LastModifiedDate,
    concat(Cast(COALESCE(a.namex, 'NoName') AS string), ' - ' , Segment , ' - ' , substr(COALESCE(StageName, 'NoStage'), 0, 8) )  AS Customer_Segment_Stage,
    trunc(Cast(o1.APPROVAL_AMOUNT * x.conversionrate AS numeric), 2)			    AS USD
FROM   
    `rax-landing-qa`.salesforce_ods.qopportunity o1 
INNER JOIN
    (
		SELECT DISTINCT Load_Date_Time_Month_Key, Account_Owner_ID,Account_Owner_Role  
		FROM `rax-abo-72-dev`.sales.eom_account_owner_snapshot  
		WHERE upper(Account_Owner_ISACTIVE) ='TRUE' and Load_Date_Time_Month_Key=ThisMonth
	) User1
ON  o1.ownerid=User1.Account_Owner_ID
AND `rax-abo-72-dev`.bq_functions.udf_yearmonth_nohyphen(closedate)=User1.Load_Date_Time_Month_Key
INNER JOIN
    `rax-abo-72-dev`.sales.dim_sales_us_team_hierarchy  Hier 
ON User1.Account_Owner_Role=Hier.Team
AND Load_Date_Time_Month_Key=Time_Month_Key
--AND lower(o1.Split_Category) NOT LIKE '%secondary'
AND Is_Active=1
LEFT JOIN exchange x
ON o1.CURRENCYISOCODE = x.ISOCODE
INNER JOIN 
    `rax-landing-qa`.salesforce_ods.qaccount a 
ON o1.accountid = a.id
WHERE  
    `rax-abo-72-dev`.bq_functions.udf_yearmonth_nohyphen(closedate) = NextMonth
    and o1.approval_amount * x.conversionrate >= 10000
    and lower(forecastcategoryname) not in ( 'omitted' )
    and lower(account_owner_role) not like '%sharepoint%'
    and lower(account_owner_role) not like '%deal center%'
   -- and ( lower(o1.final_opportunity_type) not in ( 'cloud', 'mail', 'managed cloud' )
    --or o1.final_opportunity_type is null )
    and lower(forecastcategoryname) = 'best case'
ORDER  BY ( o1.APPROVAL_AMOUNT * x.conversionrate ) DESC
limit 25;

--Top 25 Next month's Pipeline
create or replace temp table pipeline2 as
SELECT --INTO       #pipeline2
   FORMAT_DATE('%B', Date_add(current_date(), interval 1 month)) AS Month,
    forecastCategoryName,
    o1.id,
    o1.LastModifiedDate,
    concat(Cast(COALESCE(a.namex, 'NoName') AS string) ,' - ' , Segment, ' - ' , substr(COALESCE(StageName, 'NoStage'), 0, 8) )   AS Customer_Segment_Stage,
    trunc(Cast(o1.APPROVAL_AMOUNT * x.conversionrate AS numeric), 2)			    AS USD
	
FROM   
    `rax-landing-qa`.salesforce_ods.qopportunity o1 
INNER JOIN
    (
		SELECT DISTINCT Load_Date_Time_Month_Key, Account_Owner_ID,Account_Owner_Role  
		FROM `rax-abo-72-dev`.sales.eom_account_owner_snapshot  
		WHERE upper(Account_Owner_ISACTIVE) ='TRUE' and Load_Date_Time_Month_Key=ThisMonth
	) User1
ON  o1.ownerid=User1.Account_Owner_ID
AND `rax-abo-72-dev`.bq_functions.udf_yearmonth_nohyphen(closedate)=User1.Load_Date_Time_Month_Key
INNER JOIN
    `rax-abo-72-dev`.sales.dim_sales_us_team_hierarchy  Hier 
ON User1.Account_Owner_Role=Hier.Team
AND Load_Date_Time_Month_Key=Time_Month_Key
--AND lower(o1.Split_Category) NOT LIKE '%secondary'
AND Is_Active=1
LEFT JOIN 
    exchange x
ON o1.CURRENCYISOCODE = x.ISOCODE
INNER JOIN 
    `rax-landing-qa`.salesforce_ods.qaccount a 
ON o1.accountid = a.id
WHERE  
    `rax-abo-72-dev`.bq_functions.udf_yearmonth_nohyphen(closedate) = NextMonth
    and o1.approval_amount * x.conversionrate >= 10000
    and lower(forecastcategoryname) not in ( 'omitted' )
    and lower(account_owner_role) not like '%sharepoint%'
    and lower(account_owner_role) not like '%deal center%'
    --and ( lower(o1.final_opportunity_type) not in ( 'cloud', 'mail', 'managed cloud' )
    --or o1.final_opportunity_type is null )
    and lower(forecastcategoryname) = 'pipeline'
ORDER  BY 
    ( o1.APPROVAL_AMOUNT * x.conversionrate ) DESC
	limit 25;


create or replace table `rax-abo-72-dev`.report_tables.weeklyppt_growth as
SELECT--INTO       Report_Tables.dbo.WeeklyPPT_Growth
    current_date()                       AS Load_date,
    thisMonth                      AS Time_Month_Key,
    forecastCategoryName		 AS Data,
    id,
    LastModifiedDate              AS LastModified,
    Customer_Segment_Stage,
    USD
FROM   Commit
UNION all
SELECT 
    current_date()                       AS Load_date,
    thisMonth                      AS Time_Month_Key,
    forecastCategoryName		 AS Forecast_Catetory,
    id,
    LastModifiedDate              AS LastModified,
    Customer_Segment_Stage,
    USD
FROM   BestCase
UNION all
SELECT
    current_date()                       AS Load_date,
    thisMonth                      AS Time_Month_Key,
    forecastCategoryName		 AS Forecast_Catetory,
    id,
    LastModifiedDate              AS LastModified,
    Customer_Segment_Stage,
    USD
FROM   pipeline
UNION all
SELECT 
    current_date()                       AS Load_date,
    NextMonth					 AS Time_Month_Key,
    forecastCategoryName		 AS Forecast_Catetory,
    id,
    LastModifiedDate			 AS LastModified,
    Customer_Segment_Stage,
    USD
FROM  Commit2
UNION all
SELECT 
    current_date()                       AS Load_date,
    NextMonth                      AS Time_Month_Key,
    forecastCategoryName		 AS Forecast_Catetory,
    id,
    LastModifiedDate              AS LastModified,
   Customer_Segment_Stage,
    USD
FROM  BestCase2
UNION all
SELECT 
    current_date()                       AS Load_date,
    NextMonth                      AS Time_Month_Key,
    forecastCategoryName		 AS Forecast_Catetory,
    id,
    LastModifiedDate              AS LastModified,
    Customer_Segment_Stage,
    USD
FROM  pipeline2 ;

end;
