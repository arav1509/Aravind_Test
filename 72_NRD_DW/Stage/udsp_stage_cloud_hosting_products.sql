CREATE OR REPLACE PROCEDURE `rax-staging-dev.stage_two_dw.udsp_stage_cloud_hosting_products`()
BEGIN

declare v_lc_product_key int64;
-------------------------------------------------------------------------------------------------------------------
--find new prod id in ebi table;
SELECT distinct 	--into    #newlist  
    EBI_PRODUCT_OBJ_ID0 as PRODUCT_POID_ID0

FROM  
    stage_one.raw_invitemeventdetail_daily_stage 
where 
     EBI_PRODUCT_OBJ_ID0 NOT IN
				(SELECT distinct      
				    Product_ID 
				FROM  
				    stage_two_dw.stage_cloud_hosting_products 
      			);
				
set v_lc_product_key=(select ifnull(max(product_key),0)+1 from  stage_two_dw.stage_cloud_hosting_products);
INSERT INTO  
   stage_two_dw.stage_cloud_hosting_products
SELECT 
v_lc_product_key as product_key,
    CAST(POID_ID0	as int64)						    AS Product_ID,
    concat('BRM_ODS_DISC' ,'-' , cast(POID_ID0 as string) )		    AS Product_ID_NK,
	NAME											    AS Product_Name,
	NULL as Product_Name_Revised, 
	ifnull(DESCR, NAME)						  		    AS Product_Desc,
	CASE 
	WHEN ( lower(NAME) LIKE '%racker%'  OR lower(NAME) LIKE '%employee%' OR lower(ifnull(DESCR, NAME)) LIKE '%racker%')THEN 'Racker'
	--WHEN Name LIKE  '%DEVOPS_Managed%' THEN 'Managed DevOps'		-- removed _08.05.15
	--WHEN Name LIKE  '%SYSOPS_Infra%' THEN  'Managed Infra'		-- removed _08.05.15
	--WHEN Name LIKE  '%SYSOPS_Managed%' THEN 'Managed Ops'		-- removed _08.05.15
	WHEN (lower(PERMITTED) LIKE '%server%' AND lower(NAME) NOT like '%legacy%'  and  lower(NAME) not like '%racker%')  THEN  'Next Gen Servers'
	--WHEN (NAME LIKE  '%Big Data%'  OR  NAME LIKE  '%BigData%') THEN 'Next Gen Servers'
	--WHEN NAME LIKE '%Uptime Usage%' THEN  'Next Gen Servers'		-- added new value 08.05.15
	--WHEN NAME LIKE '%Teeth%' THEN  'Next Gen Servers'			-- modified to next gen 08.05.15	
	--WHEN NAME LIKE '%Server%' THEN  'Next Gen Servers'
	WHEN (lower(PERMITTED) LIKE '%objectrocket%')  THEN 'Object Rocket'
	WHEN (lower(PERMITTED) LIKE '%azure%')  THEN  'Azure'
	WHEN (lower(PERMITTED) like '%cas%') THEN  'RAS'
	WHEN (lower(PERMITTED) LIKE '%aws%') THEN  'AWS'
	--WHEN NAME LIKE '%Legacy Server%' THEN 'First Gen Servers'
	WHEN (lower(PERMITTED) LIKE '%server%' AND  lower(NAME) like '%legacy%'  and  lower(NAME) not like '%racker%')  THEN  'First Gen Servers'
	WHEN (lower(NAME) LIKE  '%cloud files%' OR  lower(NAME) LIKE  '%cfiles%' OR   lower(NAME) LIKE  '%cdn%') THEN 'Cloud Files'
	WHEN (lower(NAME) LIKE  '%professional service%' OR  lower(NAME) LIKE '%dba services%') THEN 'Professional Services'
	WHEN (lower(NAME) LIKE  '%rms%' OR  lower(PERMITTED) LIKE '%rms%' ) THEN 'RMS'
	WHEN (lower(NAME) LIKE  '%google%' OR  lower(PERMITTED) LIKE '%/mgcp%' ) THEN 'MGCP'
	WHEN (lower(PERMITTED) LIKE '%cloud_office_ent%') THEN 'Office 365'
	WHEN lower(NAME) LIKE  '%cloud glance%' THEN 'Cloud Glance'
	WHEN lower(NAME) LIKE  '%cloud queue%' THEN 'Cloud Queues'
	--WHEN NAME LIKE  '%Commit Discount%' THEN 'Discount'	
	WHEN 	lower(NAME) LIKE  '%cbs%' THEN 'Cloud Block Storage'
	WHEN 	lower(NAME) LIKE  '%cloud backup%' THEN 'Cloud Backup'
	WHEN   (lower(NAME) LIKE  '%cloud sites%' OR lower(NAME) LIKE  '%csites%'  OR lower(NAME) LIKE  '%cloud site%' ) THEN 'Cloud Sites'
	WHEN   (lower(NAME) LIKE  '%maas%' OR lower(NAME) LIKE '%monitoring%' ) THEN 'Cloud Monitoring'
	WHEN 	lower(NAME) LIKE  '%dbaas%' THEN 'Cloud Databases'
	WHEN 	lower(NAME) LIKE  '%365%' THEN 'Office 365'
	WHEN   (lower(NAME) LIKE  '%loadbalancer%' OR lower(NAME) LIKE  '%ldbal%' OR  lower(NAME) LIKE '%load balancer%') THEN 'Cloud Load Balancer'
	ELSE 
	   'Discount'		
	END											    AS Product_Reporting_Group_DESC,
    0											    AS Product_Reporting_Group_ID,
    0											    AS Is_Stand_Alone_Fee,
    0											    AS Normalize,
    'Recurring'									    AS Charge_Type,
    1											    AS Is_Global_Net_Revenue,
    1											    AS Is_Cloud_Comp_Revenue
FROM 
	`rax-landing-qa`.brm_ods.discount_t a 
inner join
    (
	SELECT distinct 	--into    #newlist  
    EBI_PRODUCT_OBJ_ID0 as PRODUCT_POID_ID0
	FROM  
		stage_one.raw_invitemeventdetail_daily_stage 
	where 
		 EBI_PRODUCT_OBJ_ID0 NOT IN
					(SELECT distinct      
						Product_ID 
					FROM  
						stage_two_dw.stage_cloud_hosting_products 
					)
	)b--#newlist b
ON a.POID_ID0 =b.PRODUCT_POID_ID0
WHERE
   PRODUCT_POID_ID0<> 0;
-------------------------------------------------------------------------------------------------------------------	
INSERT INTO     stage_two_dw.stage_cloud_hosting_products
SELECT 
v_lc_product_key as product_key,
    CAST(POID_ID0	as int64)					 AS Product_ID,
	concat('BRM_ODS' ,'-' , cast(POID_ID0 as string) )	 AS Product_ID_NK,
	LOWER(NAME) AS Product_NAME,
	NULL as Product_NAME_Revised,
	IfNULL(DESCR, LOWER(NAME))							 AS Product_Desc,
	CASE 
	when (lower(name) like '%racker%'  or lower(name) like '%employee%' or ifnull(descr, lower(name)) like '%racker%')THEN 'Racker'
	--WHEN LOWER(NAME) LIKE  '%DEVOPS_Managed%' THEN 'Managed DevOps'		-- removed _08.05.15
	--WHEN LOWER(NAME) LIKE  '%SYSOPS_Infra%' THEN  'Managed Infra'		-- removed _08.05.15
	--WHEN LOWER(NAME) LIKE  '%SYSOPS_Managed%' THEN 'Managed Ops'		-- removed _08.05.15
	when (lower(permitted) like '%server%' and lower(name) not like '%legacy%'  and  lower(name) not like '%racker%')  THEN  'Next Gen Servers'
	--WHEN (LOWER(NAME) LIKE '%Cloud Server%' OR  LOWER(NAME) LIKE  '%Next Gen%') THEN 'Next Gen Servers'
	--WHEN (LOWER(NAME) LIKE  '%Big Data%'  OR  LOWER(NAME) LIKE  '%BigData%') THEN 'Next Gen Servers'
	--WHEN LOWER(NAME) LIKE '%Uptime Usage%' THEN  'Next Gen Servers'		-- added new value 08.05.15
	--WHEN LOWER(NAME) LIKE '%Teeth%' THEN  'Next Gen Servers'			-- modified to next gen 08.05.15	
	--WHEN LOWER(NAME) LIKE '%Server%' THEN  'Next Gen Servers'
	when (lower(permitted) like '%objectrocket%')  THEN 'Object Rocket'
	when (lower(permitted) like '%azure%')  THEN  'Azure'
	when (lower(permitted) like '%cas%') THEN  'RAS'
	when (lower(permitted) like '%aws%') THEN  'AWS'
	when (lower(permitted) like '%server%' and lower(name) like '%legacy%'  and  lower(name) not like '%racker%')  THEN  'First Gen Servers'
	--WHEN LOWER(NAME) LIKE '%Legacy Server%' THEN 'First Gen Servers'
	when (lower(name) like  '%cloud files%' or  lower(name) like  '%cfiles%' or lower(name) like  '%cdn%') THEN 'Cloud Files'
	when (lower(name) like  '%professional service%' or  lower(name) like '%dba services%') THEN 'Professional Services'
	when (lower(name) like  '%rms%' or  lower(permitted) like '%rms%' ) THEN 'RMS'
	when (lower(name) like  '%google%' or  lower(permitted) like '%/mgcp%' ) THEN 'MGCP'
	when ( lower(permitted) like '%cloud_office_ent%') THEN 'Office 365'
	when lower(name) like  '%cloud glance%' THEN 'Cloud Glance'
	when lower(name) like  '%cloud queue%' THEN 'Cloud Queues'
	--WHEN LOWER(NAME) LIKE  '%Commit Discount%' THEN 'Discount'	
	when lower(name) like  '%cbs%' THEN 'Cloud Block Storage'
	when lower(name) like  '%cloud backup%' THEN 'Cloud Backup'
	when  (lower(name) like  '%cloud sites%' or lower(name) like  '%csites%'  or lower(name) like  '%cloud site%' ) THEN 'Cloud Sites'
	when ( lower(name) like  '%maas%' or lower(name) like '%monitoring%' ) THEN 'Cloud Monitoring'
	when lower(name) like  '%dbaas%' THEN 'Cloud Databases'
	when  ( lower(name) like  '%loadbalancer%' or lower(name) like  '%ldbal%' or  lower(name) like '%load balancer%') THEN 'Cloud Load Balancer'
	ELSE 
	   'N/A'
	END										 AS Product_Reporting_Group_DESC,
	0										 AS Product_Reporting_Group_ID,
	0										 AS Is_Stand_Alone_Fee,
	0										 AS Normalize,
	'Recurring'								 AS Charge_Type,
	1										 AS Is_Global_Net_Revenue,
	1										 AS Is_Cloud_Comp_Revenue
FROM 
	`rax-landing-qa`.brm_ods.product_t b 
inner join
    (
		
			SELECT distinct 	--into    #newlist  
			EBI_PRODUCT_OBJ_ID0 as PRODUCT_POID_ID0
			FROM  
				stage_one.raw_invitemeventdetail_daily_stage 
			where 
				 EBI_PRODUCT_OBJ_ID0 NOT IN
							(SELECT distinct      
								Product_ID 
							FROM  
								stage_two_dw.stage_cloud_hosting_products 
							)
			
			
	) n--#newlist n
ON b.POID_ID0 = n.PRODUCT_POID_ID0;


-------------------------------------------------------------------------------------------------------------------
 merge into  stage_two_dw.stage_cloud_hosting_products  A
   using     stage_two_dw.stage_cloud_hosting_products  B
ON A.Product_Reporting_Group_ID=B.Product_Reporting_Group_ID   
   WHEN MATCHED  AND  UPPER(A.Charge_Type)='N/A'
   THEN 
   UPDATE SET
   A.Is_Stand_Alone_Fee=B.Is_Stand_Alone_Fee,
   A.Normalize=B.Normalize,
   A.Charge_Type=B.Charge_Type,
   A.Is_Global_Net_Revenue=B.Is_Global_Net_Revenue,
   A.Is_Cloud_Comp_Revenue=B.Is_Cloud_Comp_Revenue; 
-------------------------------------------------------------------------------------------------------------------
 -- when existing discount prod id name or description have been renamed, update to new name/descr

merge into stage_two_dw.stage_cloud_hosting_products  A
using `rax-landing-qa`.brm_ods.discount_t b 
on  A.Product_ID =b.POID_ID0
when matched and ( A.Product_Name<>B.NAME AND upper(Product_ID_NK) like '%BRM%') then 
update set 
    A.Product_Name=	NAME,
    A.Product_Desc=IfNULL(DESCR, NAME);

-------------------------------------------------------------------------------------------------------------------
UPDATE	 -- when existing prod id name or description have been renamed, update to new name/descr
     stage_two_dw.stage_cloud_hosting_products  A
SET
    A.Product_Name=	NAME,
    A.Product_Desc=IFNULL(DESCR, NAME),
	A.Product_Name_Revised = 
		(CASE 
		WHEN UPPER(Product_Name) LIKE '%TEETH%' THEN REPLACE(Product_Name, 'Teeth', 'OnMetal')
		WHEN UPPER(Product_Name) LIKE '%NOVA GENERAL1%' THEN REPLACE(Product_Name, 'Nova General1', 'Performance 1')
		WHEN UPPER(Product_Name) LIKE '%NOVA IO1%' THEN REPLACE(Product_Name, 'Nova IO1', 'Performance 2')
		WHEN UPPER(Product_Name) LIKE '%DISKLESS%' THEN REPLACE(Product_Name, 'Diskless', 'NextGen')
		ELSE		Product_Name 
		END		)
FROM 
	`rax-landing-qa`.brm_ods.product_t b 
WHERE A.Product_ID =b.POID_ID0
AND
   A.Product_Name<>B.NAME
AND upper(Product_ID_NK) like '%BRM%';
-------------------------------------------------------------------------------------------------------------------
UPDATE 		-- rename server name values for nextgen/performance server uptime on new records_04.30.15
stage_two_dw.stage_cloud_hosting_products
SET Product_Name_Revised = 
		(CASE 
		WHEN UPPER(Product_Name) LIKE '%TEETH%' 			THEN REPLACE(Product_Name, 'Teeth', 'OnMetal')
		WHEN UPPER(Product_Name) LIKE '%NOVA GENERAL1%' 	THEN REPLACE(Product_Name, 'Nova General1', 'Performance 1')
		WHEN UPPER(Product_Name) LIKE '%NOVA IO1%' 		THEN REPLACE(Product_Name, 'Nova IO1', 'Performance 2')
		WHEN UPPER(Product_Name) LIKE '%DISKLESS%' 		THEN REPLACE(Product_Name, 'Diskless', 'NextGen')
		ELSE		Product_Name 
		END		)
WHERE 
 UPPER(Product_ID_NK) like 'BRM%'
 AND Product_Name_Revised IS NULL;
-------------------------------------------------------------------------------------------------------------------
 UPDATE  -- update one_time
     stage_two_dw.stage_cloud_hosting_products  A
SET
    A.Charge_Type='Non-Recurring',
	Normalize=0
  
WHERE
  (LOWER(Product_Name) like '%one%' or LOWER(Product_Name) like '%discount%'  or LOWER(Product_Desc) like '%discount%')
AND LOWER(Charge_Type) <> 'non-recurring';
-------------------------------------------------------------------------------------------------------------------
 UPDATE  -- update one_time
    stage_two_dw.stage_cloud_hosting_products  A
SET
    A.Charge_Type='Recurring'
WHERE
	UPPER(Product_Name) like '%RECURRING%'
AND UPPER(Charge_Type) <> 'RECURRING';
 -------------------------------------------------------------------------------------------------------------------
 UPDATE  -- update one_time
stage_two_dw.stage_cloud_hosting_products  A
SET
   A.Is_Stand_Alone_Fee=1 
WHERE
  LOWER(Product_Name) like '%one%' 
AND Is_Stand_Alone_Fee=0;
 -------------------------------------------------------------------------------------------------------------------
END;
