CREATE OR REPLACE PROCEDURE `rax-staging-dev.stage_two_dw.udsp_load_dim_invoice_and_attributes`()
BEGIN


declare v_Invoice_Attribute_Key int64;
declare v_invoice_key int64;
declare v_dim_invoice_load_date datetime;
declare v_dim_invoice_attribute_load_date datetime;



set v_Invoice_Attribute_Key = (select ifnull(max(Invoice_Attribute_Key),0) from `rax-datamart-dev`.corporate_dmart.dim_invoice_attribute);

set v_invoice_key = (select ifnull(max(invoice_key),0) from `rax-datamart-dev`.corporate_dmart.dim_invoice );

set v_dim_invoice_load_date = (select cast( ifnull(cast(max(Record_Created_Datetime) as datetime),cast('1970-01-01' as datetime) ) as datetime) from `rax-datamart-dev`.corporate_dmart.dim_invoice);

set v_dim_invoice_attribute_load_date = (select cast(ifnull(cast(max(Record_Created_Datetime) as datetime),cast('1970-01-01' as datetime) ) as datetime)from `rax-datamart-dev`.corporate_dmart.dim_invoice_attribute);
---------------------------------------------------------------
---dim_invoice
create or replace table stage_two_dw.stage_dim_invoice
as
select
row_number() over() as invoice_key,
invoice_nk,
invoice_source_field_nk,
bill_number,
billing_application_account_number,
unit_measure_of_code,
impact_category,
impact_type_id,
impact_type_description,
service_type,
'BRM' as source_system_name,--
'udsp_load_dim_invoice_and_attributes' as record_created_by,
CURRENT_TIMESTAMP() as record_created_datetime,
'udsp_load_dim_invoice_and_attributes' as record_updated_by,
CURRENT_TIMESTAMP() as record_updated_datetime
from
(
SELECT  Invoice_Nk 
       ,Invoice_Source_field_NK 
       ,BILL_NO Bill_Number 
       ,Brm_Account Billing_Application_Account_Number
	   ,'N/A' Unit_Measure_Of_Code
	   ,'N/A' Impact_category
	   ,0 AS Impact_Type_Id
	   ,'N/A' AS Impact_Type_Description 
	   ,'N/A' AS  Service_Type
FROM  stage_two_dw.stage_email_apps_inv_event_detail
WHERE load_date > v_dim_invoice_load_date
UNION ALL
SELECT  Invoice_Nk 
       ,'Bill_Number|Billing_Application_Account_Number|Unit_Measure_Of_Code|Impact_Category|Impact_Type_Id|Impact_Type_Description|Service_Type' Invoice_Source_field_NK 
       ,ITEM_NO Bill_Number 
       ,BRM_Account_No Billing_Application_Account_Number
	   ,'N/A' Unit_Measure_Of_Code
	   ,'N/A' Impact_category
	   ,0 AS Impact_Type_Id
	   ,'N/A' AS Impact_Type_Description 
	   ,Service_Type  AS  Service_Type
FROM  stage_two_dw.stage_credits_brm  
WHERE tblload_dtt > v_dim_invoice_load_date
UNION ALL 
SELECT  
        Invoice_Nk 
       , Invoice_Source_field_NK 
       ,BILL_NO Bill_Number 
       ,Brm_AccountNo Billing_Application_Account_Number
	   ,IFNULL(UOM,'N/A') Unit_Measure_Of_Code
	   ,IFNULL(IMPACT_CATEGORY,'N/A') Impact_category
	   ,IFNULL(EBI_IMPACT_TYPE,0) Impact_Type_Id
	   ,IMPACT_TYPE_DESCRIPTION Impact_Type_Description 
	   ,IFNULL(Service_Obj_Type,'N/A') Service_Type
FROM  stage_two_dw.stage_invitemeventdetail
WHERE tblload_dtt > v_dim_invoice_load_date
UNION ALL 
SELECT     Invoice_Nk 
       , Invoice_Source_field_NK 
       , Bill_No  Bill_Number 
       ,Brm_Account_No Billing_Application_Account_Number
	   ,IFNULL(Trx_Unit_Of_Measure_Code,'N/A') Unit_Measure_Of_Code
	   ,IFNULL(IMPACT_CATEGORY,'N/A') Impact_category
	   ,IFNULL(CAST(IMPACT_TYPE AS INT64),0) Impact_Type_Id
	   , Impact_Type_Description    
	   ,IFNULL(Service_Obj_Type,'N/A') Service_Type
FROM stage_two_dw.stage_dedicated_inv_event_detail
WHERE etldtt > v_dim_invoice_load_date
)
;
MERGE INTO `rax-datamart-dev`.corporate_dmart.dim_invoice dim
USING (
SELECT  row_number() over() AS invoice_key,
  IFNULL(Invoice_Nk,'N/A') Invoice_Nk, 
	IFNULL(Invoice_Source_field_NK, 'N/A') Invoice_Source_field_NK, 
	IFNULL(Bill_number,'N/A') Bill_number, 
	IFNULL(Billing_Application_Account_Number,'N/A') Billing_Application_Account_Number,
	IFNULL(Unit_Measure_Of_Code,'N/A') Unit_Measure_Of_Code,
	IFNULL(Impact_Category,'N/A') Impact_Category,
	IFNULL(Impact_Type_Id,0) Impact_Type_Id,
	IFNULL(Impact_Type_Description,'N/A') Impact_Type_Description,
	IFNULL(Service_Type,'N/A') Service_Type,
	'BRM'  Source_System_Name,
	'udsp_load_dim_invoice_and_attributes' AS Record_Created_By,
	CURRENT_DATETIME() AS Record_Created_Datetime,
	'udsp_load_dim_invoice_and_attributes' AS Record_Updated_By,
	CURRENT_DATETIME() AS Record_Updated_Datetime
FROM  stage_two_dw.stage_dim_invoice 
--WHERE Invoice_NK = '000369DBE0C48C3A7B215355A33B3F0A'
where Invoice_Nk = "N/A"
GROUP BY Invoice_Nk, Invoice_Source_field_NK, Bill_Number, 
Billing_Application_Account_Number, Unit_Measure_Of_Code, Impact_Category, Impact_Type_Id, 
Impact_Type_Description, Service_Type, Source_System_Name

) stg
ON dim.invoice_nk=stg.invoice_nk
WHEN MATCHED THEN
update set
 --dim.invoice_key	=	stg.invoice_key
 dim.invoice_nk	=	stg.invoice_nk
,dim.invoice_source_field_nk	=	stg.invoice_source_field_nk
,dim.bill_number	=	stg.bill_number
,dim.billing_application_account_number	=	stg.billing_application_account_number
,dim.unit_measure_of_code	=	stg.unit_measure_of_code
,dim.impact_category	=	stg.impact_category
,dim.impact_type_id	=	cast(stg.impact_type_id as int64)
,dim.impact_type_description	=	stg.impact_type_description
,dim.service_type	=	stg.service_type
,dim.source_system_name	=	stg.source_system_name
,dim.record_created_by	=	stg.record_created_by
,dim.record_created_datetime	=	CAST(stg.record_created_datetime AS DATETIME)
,dim.record_updated_by	=	stg.record_updated_by
,dim.record_updated_datetime	=	CAST( stg.record_updated_datetime AS DATETIME)
WHEN NOT MATCHED THEN
insert  (invoice_key,invoice_nk,invoice_source_field_nk,bill_number,billing_application_account_number,unit_measure_of_code,impact_category,impact_type_id,impact_type_description,service_type,source_system_name,record_created_by,record_created_datetime,record_updated_by,record_updated_datetime)
values	(v_invoice_key,invoice_nk,invoice_source_field_nk,bill_number,billing_application_account_number,unit_measure_of_code,impact_category,cast(stg.impact_type_id as int64),impact_type_description,service_type,source_system_name,record_created_by,record_created_datetime,record_updated_by,record_updated_datetime);
---------------------------------------------------------------
--dim_invoice_attribute invitemeventdetail 

CREATE OR REPLACE TABLE stage_two_dw.stg_distinct_invitemeventdetail AS
SELECT   Cast(COALESCE(event_fastlane_service_type,fastlane_impact_grp_code,'N/A') AS  STRING )  AS invoice_group_code ,
         Cast(COALESCE(event_fastlane_event_type,fastlane_impact_sub_grp_code,'N/A')AS STRING  ) AS invoice_sub_group_code ,
         Cast(COALESCE(event_fastlane_dc_id,dc_id,'N/A') AS                            STRING )   AS fastlane_data_center_id ,
         Cast(COALESCE(event_fastlane_region,'N/A') AS                                 STRING )   AS region ,
         Cast(COALESCE(event_fastlane_resource_id,res_id,'N/A') AS                     STRING )   AS resource_id ,
         Cast(COALESCE(event_fastlane_resource_name,res_name,'N/A') AS                 STRING )   AS resource_name ,
         Cast(COALESCE(event_fastlane_attr1,'N/A') AS                                  STRING  )  AS fastlane_attr1 ,
         Cast(COALESCE(event_fastlane_attr2,'N/A') AS                                  STRING  )  AS fastlane_attr2 ,
         Cast(COALESCE(event_fastlane_attr3,'N/A') AS                                  STRING )   AS fastlane_attr3 ,
         Cast(COALESCE(activity_attr4,'N/A') AS                                        STRING )   AS fastlane_attr4 ,
         Cast(COALESCE(activity_attr5,'N/A') AS                                        STRING )   AS fastlane_attr5 ,
         Cast(COALESCE(activity_attr6,'N/A') AS                                        STRING )   AS fastlane_attr6 ,
         Cast(COALESCE(activity_attr7,'N/A') AS                                        STRING )   AS fastlane_attr7 ,
         Cast(COALESCE(activity_attr8,'N/A') AS                                        STRING )   AS fastlane_attr8 ,
         Cast(COALESCE(fastlane_impact_category,'N/A') AS                              STRING )   AS fastlane_impact_category ,
         Cast(COALESCE(fastlane_impact_value,'N/A') AS                                 STRING )   AS fastlane_impact_value ,
         Cast(COALESCE(fastlane_impact_deal_code,'N/A') AS                             STRING )   AS deal_code,
         Cast( COALESCE(rate,'N/A') AS        STRING) AS rate , 
         Cast(COALESCE(ebi_rate_tag,'N/A') AS STRING) AS rate_tag , 
         Cast(COALESCE(tax_type_id ,0) AS     STRING) AS tax_type_id , 
         Cast(COALESCE(tax_element_id,0) AS   STRING) AS tax_element_id , 
         Invoice_Attribute_NK , 
         invoice_attribute_source_field_nk 	
FROM     stage_two_dw.stage_invitemeventdetail 
WHERE    tblload_dtt > v_dim_invoice_attribute_load_date
GROUP BY Cast(COALESCE(event_fastlane_service_type,fastlane_impact_grp_code,'N/A') AS   STRING) ,
         Cast(COALESCE(event_fastlane_event_type,fastlane_impact_sub_grp_code,'N/A')AS  STRING ) ,
         Cast(COALESCE(event_fastlane_dc_id,dc_id,'N/A') AS                             STRING) ,
         Cast(COALESCE(event_fastlane_region,'N/A') AS                                  STRING)  ,
         cast(COALESCE(event_fastlane_resource_id,res_id,'N/A') AS                      STRING) ,
         cast(COALESCE(event_fastlane_resource_name,res_name,'N/A') AS                  STRING)  ,
         cast(COALESCE(event_fastlane_attr1,'N/A') AS                                   STRING )  ,
         cast(COALESCE(event_fastlane_attr2,'N/A') AS                                   STRING )  ,
         cast(COALESCE(event_fastlane_attr3,'N/A') AS                                   STRING) ,
         cast(COALESCE(activity_attr4,'N/A') AS                                         STRING)  ,
         cast(COALESCE(activity_attr5,'N/A') AS                                         STRING)   ,
         cast(COALESCE(activity_attr6,'N/A') AS                                         STRING)   ,
         cast(COALESCE(activity_attr7,'N/A') AS                                         STRING)  ,
         cast(COALESCE(activity_attr8,'N/A') AS                                         STRING)  ,
         cast(COALESCE(fastlane_impact_category,'N/A') AS                               STRING)   ,
         cast(COALESCE(fastlane_impact_value,'N/A') AS                                  STRING)   ,
         cast(COALESCE(fastlane_impact_deal_code,'N/A') AS                              STRING)  , 
         cast( COALESCE(rate,'N/A') AS        STRING)  , 
         cast(COALESCE(ebi_rate_tag,'N/A') AS STRING)  , 
         cast(COALESCE(tax_type_id ,0) AS     STRING)  , 
         cast(COALESCE(tax_element_id,0) AS   STRING)  , 
         Invoice_Attribute_NK , 
         invoice_attribute_source_field_nk;

insert into `rax-datamart-dev`.corporate_dmart.dim_invoice_attribute
(Charge_Type,Deal_Code,Fastlane_Attr1,Fastlane_Attr2,Fastlane_Attr3,Fastlane_Attr4,Fastlane_Attr5,Fastlane_Attr6,Fastlane_Attr7,Fastlane_Attr8,Fastlane_Data_Center_Id,Fastlane_Impact_Category,Fastlane_Impact_Value,Invoice_Attribute_Key,Invoice_Attribute_Nk,Invoice_Attribute_Source_Field_Nk,Invoice_Group_Code,Invoice_Sub_Group_Code,Rate,Rate_Tag,Record_Created_By,Record_Created_Datetime,Record_Updated_By,Record_Updated_Datetime,Region,Resource_Id,Resource_Name,Service_Level,Tax_Element_Id,Tax_Type_Id,Usage_Record_Id)
select
Charge_Type,
Deal_Code,
Fastlane_Attr1,
Fastlane_Attr2,
Fastlane_Attr3,
Fastlane_Attr4,
Fastlane_Attr5,
Fastlane_Attr6,
Fastlane_Attr7,
Fastlane_Attr8,
Fastlane_Data_Center_Id,
Fastlane_Impact_Category,
Fastlane_Impact_Value,
v_Invoice_Attribute_Key as Invoice_Attribute_Key,
Invoice_Attribute_Nk,
Invoice_Attribute_Source_Field_Nk,
Invoice_Group_Code,
Invoice_Sub_Group_Code,
Rate,
Rate_Tag,
'udsp_load_dim_invoice_and_attributes' as Record_Created_By,
CURRENT_TIMESTAMP() Record_Created_Datetime,
'udsp_load_dim_invoice_and_attributes' Record_Updated_By,
CURRENT_TIMESTAMP() Record_Updated_Datetime,
Region,
Resource_Id,
Resource_Name,
Service_Level,
Tax_Element_Id,
Tax_Type_Id,
"N/A" as Usage_Record_Id
from(
SELECT 
 IFNULL(invoice_group_code,'N/A')  AS invoice_group_code 
,IFNULL(invoice_sub_group_code,'N/A') AS invoice_sub_group_code 
,IFNULL(fastlane_data_center_id, 'N/A') AS fastlane_data_center_id
,IFNULL(region,'N/A')  AS region  
,IFNULL(resource_id, 'N/A') AS resource_id 
,IFNULL(resource_name, 'N/A' ) AS resource_name 
,IFNULL(fastlane_attr1, 'N/A' ) AS fastlane_attr1 
,IFNULL(fastlane_attr2, 'N/A' ) AS fastlane_attr2 
,IFNULL(fastlane_attr3, 'N/A') AS fastlane_attr3  
,IFNULL(fastlane_attr4,'N/A' ) AS fastlane_attr4 
,IFNULL(fastlane_attr5, 'N/A') AS fastlane_attr5 
,IFNULL(fastlane_attr6, 'N/A') AS fastlane_attr6  
,IFNULL(fastlane_attr7, 'N/A'  )  AS fastlane_attr7
,IFNULL(fastlane_attr8, 'N/A' ) AS fastlane_attr8 
,IFNULL(fastlane_impact_category, 'N/A' ) fastlane_impact_category
,IFNULL(fastlane_impact_value, 'N/A' ) fastlane_impact_value
,CASE 	 WHEN LOWER(Fastlane_Attr1) LIKE 'chargetype|%' THEN SUBSTR(Fastlane_Attr1,STRPOS(Fastlane_Attr1,'|') + 1,LENGTH(Fastlane_Attr1))
         WHEN LOWER(Fastlane_ATTR2) LIKE 'chargetype|%' THEN SUBSTR(Fastlane_ATTR2,STRPOS(Fastlane_ATTR2,'|') + 1,LENGTH(Fastlane_ATTR2))
         WHEN LOWER(Fastlane_ATTR3) LIKE 'chargetype|%' THEN SUBSTR(Fastlane_ATTR3,STRPOS(Fastlane_ATTR3,'|') + 1,LENGTH(Fastlane_ATTR3))
         WHEN LOWER(Fastlane_ATTR4) LIKE 'chargetype|%' THEN SUBSTR(Fastlane_ATTR4,STRPOS(Fastlane_ATTR4,'|') + 1,LENGTH(Fastlane_ATTR4))
		 WHEN LOWER(Fastlane_ATTR5) LIKE 'chargetype|%' THEN SUBSTR(Fastlane_ATTR5,STRPOS(Fastlane_ATTR5,'|') + 1,LENGTH(Fastlane_ATTR5))
		 WHEN LOWER(Fastlane_ATTR6) LIKE 'chargetype|%' THEN SUBSTR(Fastlane_ATTR6,STRPOS(Fastlane_ATTR6,'|') + 1,LENGTH(Fastlane_ATTR6))
		 WHEN LOWER(Fastlane_ATTR7) LIKE 'chargetype|%' THEN SUBSTR(Fastlane_ATTR7,STRPOS(Fastlane_ATTR7,'|') + 1,LENGTH(Fastlane_ATTR7))
		 WHEN LOWER(Fastlane_ATTR8) LIKE 'chargetype|%' THEN SUBSTR(Fastlane_ATTR8,STRPOS(Fastlane_ATTR8,'|') + 1,LENGTH(Fastlane_ATTR8))		
		 END AS Charge_Type 
,CASE    WHEN UPPER(Fastlane_ATTR1) LIKE 'SERVICELEVEL|%' THEN SUBSTR(Fastlane_ATTR1,STRPOS(Fastlane_ATTR1,'|') + 1,LENGTH(Fastlane_ATTR1))
         WHEN UPPER(Fastlane_ATTR2) LIKE 'SERVICELEVEL|%' THEN SUBSTR(Fastlane_ATTR2,STRPOS(Fastlane_ATTR2,'|') + 1,LENGTH(Fastlane_ATTR2))
         WHEN UPPER(Fastlane_ATTR3) LIKE 'SERVICELEVEL|%' THEN SUBSTR(Fastlane_ATTR3,STRPOS(Fastlane_ATTR3,'|') + 1,LENGTH(Fastlane_ATTR3))
         WHEN UPPER(Fastlane_ATTR4) LIKE 'SERVICELEVEL|%' THEN SUBSTR(Fastlane_ATTR4,STRPOS(Fastlane_ATTR4,'|') + 1,LENGTH(Fastlane_ATTR4))
		 WHEN UPPER(Fastlane_ATTR5) LIKE 'SERVICELEVEL|%' THEN SUBSTR(Fastlane_ATTR5,STRPOS(Fastlane_ATTR5,'|') + 1,LENGTH(Fastlane_ATTR5))
		 WHEN UPPER(Fastlane_ATTR6) LIKE 'SERVICELEVEL|%' THEN SUBSTR(Fastlane_ATTR6,STRPOS(Fastlane_ATTR6,'|') + 1,LENGTH(Fastlane_ATTR6))
		 WHEN UPPER(Fastlane_ATTR7) LIKE 'SERVICELEVEL|%' THEN SUBSTR(Fastlane_ATTR7,STRPOS(Fastlane_ATTR7,'|') + 1,LENGTH(Fastlane_ATTR7))
		 WHEN UPPER(Fastlane_ATTR8) LIKE 'SERVICELEVEL|%' THEN SUBSTR(Fastlane_ATTR8,STRPOS(Fastlane_ATTR8,'|') + 1,LENGTH(Fastlane_ATTR8))		
		 END AS Service_Level 
,IFNULL(deal_code, 'N/A') deal_code
,IFNULL(rate, 'N/A') rate
,IFNULL(rate_tag, 'N/A') rate_tag 
,IFNULL(tax_type_id, '0' ) tax_type_id
,IFNULL(tax_element_id, '0')  tax_element_id
,IFNULL(Invoice_Attribute_NK, 'N/A' ) Invoice_Attribute_NK
,IFNULL(invoice_attribute_source_field_nk,'N/A') invoice_attribute_source_field_nk
FROM stage_two_dw.stg_distinct_invitemeventdetail
WHERE Invoice_Attribute_NK = "N/A"
);

---------------------------------------------------------------
--dim_invoice_attribute dedicated_inv_event_detail

create or replace table stage_two_dw.stg_distinct_dedicated_inveventdetail as 
SELECT 
  CAST(COALESCE(EVENT_FASTLANE_SERVICE_TYPE,FASTLANE_INV_GRP_CODE,'N/A') AS STRING ) AS Invoice_Group_Code
, CAST( COALESCE(EVENT_FASTLANE_EVENT_TYPE,FASTLANE_INV_SUB_GRP_CODE,'N/A')  AS STRING ) AS Invoice_Sub_Group_Code
, COALESCE(CAST( EVENT_FASTLANE_DC_ID AS STRING),'N/A') AS Fastlane_Data_Center_Id
, COALESCE(CAST( EVENT_FASTLANE_REGION AS STRING),'N/A') AS Region 
, COALESCE(CAST( EVENT_FASTLANE_RESOURCE_ID AS STRING),'N/A') AS Resource_Id
, COALESCE(CAST( EVENT_FASTLANE_RESOURCE_NAME AS STRING),'N/A') AS Resource_Name
, COALESCE(CAST(EVENT_FASTLANE_ATTR1 AS STRING ),'N/A')  AS Fastlane_Attr1
, COALESCE(CAST(EVENT_FASTLANE_ATTR2 AS STRING),'N/A') AS Fastlane_Attr2
, COALESCE(CAST(EVENT_FASTLANE_ATTR3 AS STRING),'N/A')   Fastlane_Attr3
, COALESCE(CAST(ACTIVITY_ATTR4 AS STRING),'N/A')      Fastlane_Attr4      
, COALESCE(CAST(ACTIVITY_ATTR5 AS STRING),'N/A')     Fastlane_Attr5          
, COALESCE(CAST(ACTIVITY_ATTR6 AS STRING),'N/A')   Fastlane_Attr6                 
, 'N/A' AS  Fastlane_Attr7
, 'N/A' AS  Fastlane_Attr8
, COALESCE(CAST(FASTLANE_IMPACT_CATEGORY AS STRING),'N/A') AS Fastlane_Impact_Category
, COALESCE(CAST(FASTLANE_IMPACT_VALUE AS STRING),'N/A')    AS Fastlane_Impact_Value   
, COALESCE(CAST(FASTLANE_INV_DEAL_CODE AS STRING),'N/A')  AS     Deal_Code   
--,  COALESCE(EVENT_FASTLANE_RECORD_ID,Record_Id,'N/A')  
, COALESCE (CAST(RATE AS STRING),'N/A')      AS Rate  
, COALESCE(CAST(EBI_rate_tag AS STRING),'N/A')  AS   Rate_Tag
, COALESCE(CAST(Tax_Type_Id AS STRING),'N/A')    AS Tax_Type_Id
, COALESCE(CAST(TAX_ELEMENT_ID AS STRING),'N/A') AS 	Tax_Element_Id 
, Invoice_Attribute_NK 
, 	Invoice_Attribute_Source_field_NK 	 
FROM stage_two_dw.stage_dedicated_inv_event_detail
WHERE etldtt > v_dim_invoice_attribute_load_date
GROUP BY 
  CAST(COALESCE(EVENT_FASTLANE_SERVICE_TYPE,FASTLANE_INV_GRP_CODE,'N/A') AS STRING )
, CAST( COALESCE(EVENT_FASTLANE_EVENT_TYPE,FASTLANE_INV_SUB_GRP_CODE,'N/A')  AS STRING )
, COALESCE(CAST( EVENT_FASTLANE_DC_ID AS STRING),'N/A')
, COALESCE(CAST( EVENT_FASTLANE_REGION AS STRING),'N/A')
, COALESCE(CAST( EVENT_FASTLANE_RESOURCE_ID AS STRING),'N/A')
, COALESCE(CAST( EVENT_FASTLANE_RESOURCE_NAME AS STRING),'N/A')
, COALESCE(CAST(EVENT_FASTLANE_ATTR1 AS STRING ),'N/A')
, COALESCE(CAST(EVENT_FASTLANE_ATTR2 AS STRING),'N/A')
, COALESCE(CAST(EVENT_FASTLANE_ATTR3 AS STRING),'N/A')   
, COALESCE(CAST(ACTIVITY_ATTR4 AS STRING),'N/A')            
, COALESCE(CAST(ACTIVITY_ATTR5 AS STRING),'N/A')                
, COALESCE(CAST(ACTIVITY_ATTR6 AS STRING),'N/A')                    
, COALESCE(CAST(FASTLANE_IMPACT_CATEGORY AS STRING),'N/A')
, COALESCE(CAST(FASTLANE_IMPACT_VALUE AS STRING),'N/A')      
, COALESCE(CAST(FASTLANE_INV_DEAL_CODE AS STRING),'N/A')       
--,  COALESCE(EVENT_FASTLANE_RECORD_ID,Record_Id,'N/A')  
, COALESCE (CAST(RATE AS STRING),'N/A') 
, COALESCE(CAST(EBI_rate_tag AS STRING),'N/A')   
, COALESCE(CAST(Tax_Type_Id AS STRING),'N/A')   
, COALESCE(CAST(TAX_ELEMENT_ID AS STRING),'N/A')  	
, Invoice_Attribute_NK 
, Invoice_Attribute_Source_field_NK;

insert into `rax-datamart-dev`.corporate_dmart.dim_invoice_attribute
(Charge_Type,Deal_Code,Fastlane_Attr1,Fastlane_Attr2,Fastlane_Attr3,Fastlane_Attr4,Fastlane_Attr5,Fastlane_Attr6,Fastlane_Attr7,Fastlane_Attr8,Fastlane_Data_Center_Id,Fastlane_Impact_Category,Fastlane_Impact_Value,Invoice_Attribute_Key,Invoice_Attribute_Nk,Invoice_Attribute_Source_Field_Nk,Invoice_Group_Code,Invoice_Sub_Group_Code,Rate,Rate_Tag,Record_Created_By,Record_Created_Datetime,Record_Updated_By,Record_Updated_Datetime,Region,Resource_Id,Resource_Name,Service_Level,Tax_Element_Id,Tax_Type_Id,Usage_Record_Id)
select 
Charge_Type,
Deal_Code,
Fastlane_Attr1,
Fastlane_Attr2,
Fastlane_Attr3,
Fastlane_Attr4,
Fastlane_Attr5,
Fastlane_Attr6,
Fastlane_Attr7,
Fastlane_Attr8,
Fastlane_Data_Center_Id,
Fastlane_Impact_Category,
Fastlane_Impact_Value,
v_Invoice_Attribute_Key as Invoice_Attribute_Key,
Invoice_Attribute_Nk,
Invoice_Attribute_Source_Field_Nk,
Invoice_Group_Code,
Invoice_Sub_Group_Code,
Rate,
Rate_Tag,
'udsp_load_dim_invoice_and_attributes' as Record_Created_By,
CURRENT_TIMESTAMP() Record_Created_Datetime,
'udsp_load_dim_invoice_and_attributes' Record_Updated_By,
CURRENT_TIMESTAMP() Record_Updated_Datetime,
Region,
Resource_Id,
Resource_Name,
Service_Level,
Tax_Element_Id,
Tax_Type_Id,
"N/A" as Usage_Record_Id
from(
SELECT 
 IFNULL(invoice_group_code,'N/A')  AS invoice_group_code 
,IFNULL(invoice_sub_group_code,'N/A') AS invoice_sub_group_code 
,IFNULL(fastlane_data_center_id, 'N/A') AS fastlane_data_center_id
,IFNULL(region,'N/A')  AS region  
,IFNULL(resource_id, 'N/A') AS resource_id 
,IFNULL(resource_name, 'N/A' ) AS resource_name 
,IFNULL(fastlane_attr1, 'N/A' ) AS fastlane_attr1 
,IFNULL(fastlane_attr2, 'N/A' ) AS fastlane_attr2 
,IFNULL(fastlane_attr3, 'N/A') AS fastlane_attr3  
,IFNULL(fastlane_attr4,'N/A' ) AS fastlane_attr4 
,IFNULL(fastlane_attr5, 'N/A') AS fastlane_attr5 
,IFNULL(fastlane_attr6, 'N/A') AS fastlane_attr6  
,IFNULL(fastlane_attr7, 'N/A'  )  AS fastlane_attr7
,IFNULL(fastlane_attr8, 'N/A' ) AS fastlane_attr8 
,IFNULL(fastlane_impact_category, 'N/A' ) fastlane_impact_category
,IFNULL(fastlane_impact_value, 'N/A' ) fastlane_impact_value
,CASE    WHEN LOWER(Fastlane_Attr1) LIKE 'chargetype|%' THEN SUBSTR(Fastlane_Attr1,STRPOS(Fastlane_Attr1,'|') + 1,LENGTH(Fastlane_Attr1))
         WHEN LOWER(Fastlane_ATTR2) LIKE 'chargetype|%' THEN SUBSTR(Fastlane_ATTR2,STRPOS(Fastlane_ATTR2,'|') + 1,LENGTH(Fastlane_ATTR2))
         WHEN LOWER(Fastlane_ATTR3) LIKE 'chargetype|%' THEN SUBSTR(Fastlane_ATTR3,STRPOS(Fastlane_ATTR3,'|') + 1,LENGTH(Fastlane_ATTR3))
         WHEN LOWER(Fastlane_ATTR4) LIKE 'chargetype|%' THEN SUBSTR(Fastlane_ATTR4,STRPOS(Fastlane_ATTR4,'|') + 1,LENGTH(Fastlane_ATTR4))
		 WHEN LOWER(Fastlane_ATTR5) LIKE 'chargetype|%' THEN SUBSTR(Fastlane_ATTR5,STRPOS(Fastlane_ATTR5,'|') + 1,LENGTH(Fastlane_ATTR5))
		 WHEN LOWER(Fastlane_ATTR6) LIKE 'chargetype|%' THEN SUBSTR(Fastlane_ATTR6,STRPOS(Fastlane_ATTR6,'|') + 1,LENGTH(Fastlane_ATTR6))
		 WHEN LOWER(Fastlane_ATTR7) LIKE 'chargetype|%' THEN SUBSTR(Fastlane_ATTR7,STRPOS(Fastlane_ATTR7,'|') + 1,LENGTH(Fastlane_ATTR7))
		 WHEN LOWER(Fastlane_ATTR8) LIKE 'chargetype|%' THEN SUBSTR(Fastlane_ATTR8,STRPOS(Fastlane_ATTR8,'|') + 1,LENGTH(Fastlane_ATTR8))		
		 END AS Charge_Type 
,CASE WHEN 	  UPPER(Fastlane_ATTR1) LIKE 'SERVICELEVEL|%' THEN SUBSTR(Fastlane_ATTR1,STRPOS(Fastlane_ATTR1,'|') + 1,LENGTH(Fastlane_ATTR1))
         WHEN UPPER(Fastlane_ATTR2) LIKE 'SERVICELEVEL|%' THEN SUBSTR(Fastlane_ATTR2,STRPOS(Fastlane_ATTR2,'|') + 1,LENGTH(Fastlane_ATTR2))
         WHEN UPPER(Fastlane_ATTR3) LIKE 'SERVICELEVEL|%' THEN SUBSTR(Fastlane_ATTR3,STRPOS(Fastlane_ATTR3,'|') + 1,LENGTH(Fastlane_ATTR3))
         WHEN UPPER(Fastlane_ATTR4) LIKE 'SERVICELEVEL|%' THEN SUBSTR(Fastlane_ATTR4,STRPOS(Fastlane_ATTR4,'|') + 1,LENGTH(Fastlane_ATTR4))
		 WHEN UPPER(Fastlane_ATTR5) LIKE 'SERVICELEVEL|%' THEN SUBSTR(Fastlane_ATTR5,STRPOS(Fastlane_ATTR5,'|') + 1,LENGTH(Fastlane_ATTR5))
		 WHEN UPPER(Fastlane_ATTR6) LIKE 'SERVICELEVEL|%' THEN SUBSTR(Fastlane_ATTR6,STRPOS(Fastlane_ATTR6,'|') + 1,LENGTH(Fastlane_ATTR6))
		 WHEN UPPER(Fastlane_ATTR7) LIKE 'SERVICELEVEL|%' THEN SUBSTR(Fastlane_ATTR7,STRPOS(Fastlane_ATTR7,'|') + 1,LENGTH(Fastlane_ATTR7))
		 WHEN UPPER(Fastlane_ATTR8) LIKE 'SERVICELEVEL|%' THEN SUBSTR(Fastlane_ATTR8,STRPOS(Fastlane_ATTR8,'|') + 1,LENGTH(Fastlane_ATTR8))		
		 END AS Service_Level 
,IFNULL(deal_code, 'N/A') deal_code
,IFNULL(rate, 'N/A') rate
,IFNULL(rate_tag, 'N/A') rate_tag 
,IFNULL(tax_type_id, 'N/A' ) tax_type_id
,IFNULL(tax_element_id, 'N/A')  tax_element_id
,IFNULL(Invoice_Attribute_NK, 'N/A' ) Invoice_Attribute_NK
,IFNULL(invoice_attribute_source_field_nk,'N/A') invoice_attribute_source_field_nk
FROM stage_two_dw.stg_distinct_dedicated_inveventdetail 
where UPPER(Invoice_Attribute_NK) = "N/A"
);


END;
