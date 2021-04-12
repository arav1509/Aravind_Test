CREATE OR REPLACE PROCEDURE `rax-staging-dev.stage_one.udsp_bill_invoice_staging_onetime`()
BEGIN          
	---------------------------------------------------------------------------------------          
	DECLARE jobdate DATETIME; 
	DECLARE getdate DATETIME; 
	DECLARE maxdate DATETIME; 
	DECLARE getdate_unix INT64; 
	-- DECLARE tsql STRING; 
	-- DECLARE tsql1 STRING; 
	-- DECLARE tsql2 STRING;      
	---------------------------------------------------------------------------------------   
	SET maxdate =  COALESCE((CAST((SELECT MAX(bill_mod_date) FROM stage_one.raw_invitemeventdetail_daily_stage) AS DATETIME )), (SELECT MAX(tblload_dtt) FROM stage_two_dw.stage_invitemeventdetail ));  
	SET jobdate = cast(maxdate AS DATETIME)   ;
	SET getdate = jobdate  ;        
	SET getdate_unix = unix_seconds(CAST(getdate AS TIMESTAMP));
	--DATEDIFF(second,{d '1970-01-01'},GETDATE )      ;
	---------------------------------------------------------------------------------------   
	-- SELECT MAXDATE  
	-- SELECT jobdate          
	-- SELECT GETDATE          
	-- SELECT GETDATE_UNIX      
	--*********************************************************************************************************************   
 CREATE OR REPLACE TABLE stage_one.raw_daily_bill_poid_staging  AS
SELECT *
FROM stage_one.raw_daily_bill_poid_staging
WHERE true;

	-- EXEC Drop_Indexes__Daily_BILL_POID_staging  
	--*********************************************************************************************************************   
	--Daily_BILL_POID_staging       
	--Set TSQL=' 
	INSERT INTO   `rax-staging-dev.stage_one.raw_daily_bill_poid_staging` 
	SELECT DISTINCT  
		bill_no,          
		bill_poid_id0,          
		current_total,          
		total_due,          
		adjusted,          
		disputed,          
		due,          
		recvd,          
		writeoff,          
		transferred,          
		currency              as currency_id ,   -- added field 12.07.15jcm          
		DATE(TIMESTAMP_SECONDS(cast(start_t as INT64))) AS bill_start_date, 
		--CAST(dateadd(ss,start_t,''1970-01-01'') as date)      AS BILL_START_DATE ,   
		-- CAST(dateadd(ss,end_t, ''1970-01-01'') as date)     
		DATE(TIMESTAMP_SECONDS(cast(end_t as INT64))) AS bill_end_date ,          
		-- CAST(dateadd(ss,mod_t, ''1970-01-01'') as date)      
		DATE(TIMESTAMP_SECONDS(cast(mod_t as INT64))) AS bill_mod_date ,          
		bill_account_poid_id0,          
		bill_account_no,          
		SUBSTR(bill_account_no, STRPOS(bill_account_no,'-')+1,64)   AS bill_account_id,          
		bill_gl_segment,   
		CURRENT_DATETIME()              AS tbl_load_date,
   bill_created_date
	FROM (
	SELECT            
		b.bill_no,          
		b.poid_id0      as bill_poid_id0,          
		b.current_total,          
		b.total_due,          
		b.adjusted,          
		b.disputed,          
		b.due,          
		b.recvd,          
		b.writeoff,          
		b.transferred,          
		b.currency,          
		b.start_t,          
		b.end_t,           
		b.mod_t,       
		a.poid_id0      as bill_account_poid_id0,          
		a.account_no     as bill_account_no,          
		a.gl_segment     as bill_gl_segment,
    b.created_t as bill_created_date
	FROM `rax-landing-qa.brm_ods.bill_t` b     
	INNER JOIN `rax-landing-qa.brm_ods.account_t` a  
		ON b.account_obj_id0 = a.poid_id0   
	WHERE b.mod_t >=  getdate_unix 
	AND lower(a.gl_segment) LIKE ('%cloud%')        
	);
	--*********************************************************************************************************************   
	--PRINT TSQL  
	-- EXEC (TSQL)     
	--*********************************************************************************************************************          
	-- CREATE INDEX IX_BILL_POID_ID0 ON Daily_BILL_POID_staging(BILL_POID_ID0)    
	--*********************************************************************************************************************    
	CREATE OR REPLACE TABLE stage_one.raw_ssis_invoice_load_step1  AS
SELECT * FROM stage_one.raw_ssis_invoice_load_step1 WHERE TRUE; 
	-- EXEC Drop_Indexes__SSIS_Invoice_Load_Step1    
	--*********************************************************************************************************************     
	-- Set TSQL2='   
	INSERT INTO stage_one.raw_ssis_invoice_load_step1    
	SELECT     
		account_poid_id0,          
		account_no            AS brm_accountno,          
		SUBSTR(account_no, STRPOS(account_no,'-')+1,64) AS account_id,          
		gl_segment,    
		bill_poid_id0,          
		bill_no,          
		bill_start_date,          
		bill_end_date,          
		bill_mod_date ,          
		current_total,          
		total_due,          
		adjusted,          
		disputed,          
		due,          
		recvd,          
		writeoff,          
		transferred,          
		currency_id,       
		item_poid_id0,          
		--CAST(dateadd(ss,effective_t, ''1970-01-01'') as date) 
		DATE(TIMESTAMP_SECONDS(CAST(effective_t AS INT64))) AS item_effective_date ,          
		--CAST(dateadd(ss,mod_t, ''1970-01-01'') as date)   
		DATE(TIMESTAMP_SECONDS(CAST(mod_t AS INT64))) AS item_mod_date ,          
		name             AS item_name,          
		status             AS item_status,          
		service_obj_type          AS service_obj_type,          
		poid_type            AS item_type,          
		CURRENT_DATETIME()            AS tbl_load_date,
    bill_created_date
	FROM ( --OPENQUERY(EBI-ODS-CORE, ''    
	SELECT     
		acct.poid_id0     AS account_poid_id0,         
		acct.account_no,          
		acct.gl_segment     AS gl_segment,          
		b.bill_poid_id0,          
		b.bill_no,          
		b.bill_start_date,          
		b.bill_end_date,          
		b.bill_mod_date ,          
		b.current_total,          
		b.total_due,          
		b.adjusted,          
		b.disputed,          
		b.due,          
		b.recvd,          
		b.writeoff,          
		b.transferred,          
		b.currency_id,         
		i.poid_id0      AS item_poid_id0,          
		i.effective_t,          
		i.mod_t,          
		i.name,          
		i.status,          
		i.service_obj_type,          
		i.poid_type,
    b.bill_created_date as bill_created_date
	FROM stage_one.raw_daily_bill_poid_staging b    
	INNER JOIN `rax-landing-qa`.brm_ods.item_t i  
		ON b.bill_poid_id0 = i.bill_obj_id0   
	INNER JOIN  `rax-landing-qa`.brm_ods.account_t acct 
		ON i.account_obj_id0= acct.poid_id0  
	WHERE lower(acct.gl_segment) LIKE ('%cloud%')     
	AND i.mod_t >= getdate_unix   
	AND (COALESCE(i.bill_obj_id0,0)+COALESCE(i.ar_bill_obj_id0,0)) <> 0  
	)    ;
	--*********************************************************************************************************************   
	--PRINT TSQL2  
	-- EXEC (TSQL2)     
	--*********************************************************************************************************************     
	-- CREATE INDEX IX_ITEM_POID_ID0 ON SSIS_Invoice_Load_Step1(ITEM_POID_ID0)      
	-- CREATE INDEX IX_ACCOUNT_POID_ID0 ON SSIS_Invoice_Load_Step1(ACCOUNT_POID_ID0)  
	-- CREATE INDEX IX_ITEM_STATUS ON SSIS_Invoice_Load_Step1(ITEM_STATUS)  
	--*********************************************************************************************************************    
	CREATE OR REPLACE TABLE stage_one.raw_ssis_event_initial AS
SELECT * FROM stage_one.raw_ssis_event_initial WHERE TRUE;   
	-- EXEC Drop_Indexes__SSIS_Event_Initial_Load    
	--*********************************************************************************************************************    
	-- Set TSQL1='   
	INSERT INTO stage_one.raw_ssis_event_initial    
	SELECT DISTINCT       
		event_poid_id0,  
		item_obj_id0         AS event_item_obj_id0,  
		-- CAST(dateadd(ss,start_t, ''1970-01-01'') as date)  
		DATE(TIMESTAMP_SECONDS(CAST(start_t AS INT64))) AS event_start_date,  
		-- CAST( dateadd(ss,end_t, ''1970-01-01'') as date)  
		DATE(TIMESTAMP_SECONDS(CAST(end_t AS INT64))) AS event_end_date,  
		-- CAST( dateadd(ss,mod_t, ''1970-01-01'') as date)  
		DATE(TIMESTAMP_SECONDS(CAST(mod_t AS INT64))) AS event_mod_date,  
		event_account_poid,  
		service_obj_type        AS service_type,  
		item_obj_type,  
		poid_type          AS event_type,  
		rerate_obj_id0,  
		batch_id,  
		name           AS event_name,  
		sys_descr          AS event_sys_descr,  
		rum_name          AS event_rum_name,  
		-- CAST(dateadd(ss,CREATED_T,''1970-01-01'') as date)  
		DATE(TIMESTAMP_SECONDS(CAST(created_t AS INT64))) AS event_created_date,  
		-- CAST(dateadd(ss,EARNED_START_T,''1970-01-01'') as date) 
		DATE(TIMESTAMP_SECONDS(CAST(earned_start_t AS INT64))) AS event_earned_start_date,  
		-- CAST(dateadd(ss,EARNED_END_T,''1970-01-01'') as date) 
		DATE(TIMESTAMP_SECONDS(CAST(earned_end_t AS INT64))) AS event_earned_end_date,  
		CURRENT_DATETIME()          AS tbl_load_date,
    service_obj_id0,
    bill_created_date
	FROM (	--OPENQUERY(EBI-ODS-CORE, ''   
	SELECT     
		e.poid_id0      AS event_poid_id0,  
		e.item_obj_id0,  
		e.account_obj_id0    AS event_account_poid,      
		e.start_t,  
		e.end_t,   
		e.mod_t,   
		e.service_obj_type,  
		e.poid_type,  
		e.rerate_obj_id0,  
		e.batch_id,  
		e.item_obj_type,  
		e.name,  
		e.sys_descr,  
		e.rum_name,  
		e.created_t,  
		e.earned_start_t,   
		e.earned_end_t,
    e.Service_Obj_Id0,
    A.bill_created_date
	 FROM `rax-landing-qa`.brm_ods.event_t e           
	INNER JOIN stage_one.raw_ssis_invoice_load_step1 A     
		ON e.item_obj_id0 = a.item_poid_id0          
	INNER JOIN `rax-landing-qa`.brm_ods.account_t acct   
		ON e.account_obj_id0 = acct.poid_id0       
	WHERE lower(acct.gl_segment) LIKE ('%cloud%')      
		AND item_status<> 1    
	);  
	--*********************************************************************************************************************   
	--PRINT TSQL1  
	-- EXEC (TSQL1)     
	-- --*********************************************************************************************************************          
	-- CREATE INDEX IX_EVENT_POID_ID0 ON SSIS_Event_Initial(EVENT_POID_ID0)    
	-- CREATE INDEX IX_EVENT_Item_Obj_Id0 ON SSIS_Event_Initial(EVENT_Item_Obj_Id0)   
	-- CREATE INDEX IX_EVENT_ACCOUNT_POID ON SSIS_Event_Initial(EVENT_ACCOUNT_POID)  
	-- CREATE INDEX IX_EVENT_TYPE ON SSIS_Event_Initial(EVENT_TYPE)   
	-- CREATE INDEX IX_RERATE_OBJ_ID0 ON SSIS_Event_Initial(RERATE_OBJ_ID0)   
	-- CREATE INDEX IX_BATCH_ID ON SSIS_Event_Initial(BATCH_ID)   
	-- CREATE INDEX IX_SERVICE_TYPE ON SSIS_Event_Initial(SERVICE_TYPE)   
	-- CREATE INDEX IX_ITEM_OBJ_TYPE ON SSIS_Event_Initial(ITEM_OBJ_TYPE)   
	--*********************************************************************************************************************    
	CREATE OR REPLACE TABLE stage_one.raw_ssis_event_load_step2 AS
SELECT * FROM stage_one.raw_ssis_event_load_step2 WHERE TRUE;  
	-- EXEC Drop_Indexes__SSIS_Event_Load_Step2    
	--*********************************************************************************************************************     
	INSERT INTO  stage_one.raw_ssis_event_load_step2     
	SELECT DISTINCT       
		event_poid_id0,  
		event_item_obj_id0,  
		event_start_date,  
		event_end_date,  
		event_mod_date,  
		event_account_poid,  
		service_type,  
		event_type,  
		rerate_obj_id0,  
		batch_id,  
		event_name,  
		event_sys_descr,  
		event_rum_name,  
		event_created_date,  
		event_earned_start_date,  
		event_earned_end_date,  
		inv_grp_code         	AS activity_service_type,  
		inv_sub_grp_code        AS activity_event_type,   
		record_id          		AS activity_record_id,   
		data_center_id          AS activity_dc_id,    
		region          		AS activity_region,   
		resource_id         	AS activity_resource_id,  
		resource_name         	AS activity_resource_name, 
		attr1           		AS activity_attr1,    
		attr2           		AS activity_attr2,   
		attr3           		AS activity_attr3,    
		CURRENT_DATETIME()      AS Tbl_Load_Date,  
		COALESCE(CAST(activity_is_backbill AS INT64),0)      AS activity_is_backbill,
    Attr4,    
	Attr5,    
	Attr6,
	Service_Obj_Id0,
	Bill_Created_Date
	FROM (
	-- OPENQUERY(EBI-ODS-CORE, '    
	SELECT     
		event_poid_id0,  
		event_item_obj_id0,  
		event_start_date,  
		event_end_date,  
		event_mod_date,  
		event_account_poid,  
		service_type,  
		event_type,  
		rerate_obj_id0,  
		batch_id,  
		event_name,  
		event_sys_descr,  
		event_rum_name,  
		event_created_date,  
		event_earned_start_date,  
		event_earned_end_date,  
		fastlane.inv_grp_code,   	
		fastlane.inv_sub_grp_code,  
		fastlane.record_id,   
		fastlane.data_center_id,   
		fastlane.region,     
		fastlane.resource_id,    
		fastlane.resource_name,   
		fastlane.attr1,    
		fastlane.attr2,     
		fastlane.attr3,      
		backbill_flag     AS activity_is_backbill,
   fastlane.Attr4,    
	fastlane.Attr5,    
	fastlane.Attr6,
	Service_Obj_Id0,
	E.Bill_Created_Date
	FROM  stage_one.raw_ssis_event_initial e     
	LEFT OUTER JOIN  `rax-landing-qa`.brm_ods.event_act_rax_fastlane_t AS fastlane 
		ON e.event_poid_id0 = fastlane.obj_id0    
	WHERE e.rerate_obj_id0 = 0    
	AND (e.batch_id IS NULL  OR lower(e.batch_id) NOT LIKE 'rerating%')   
	AND (lower(e.event_type) LIKE '/event/delayed/rax/cloud/%'  
	OR lower(e.event_type)= '/event/billing/cycle/discount'  
	OR lower(e.event_type)='/event/billing/cycle/tax'  
	OR lower(e.event_type)= '/event/billing/cycle/fold' 
	OR lower(e.event_type) LIKE '%/event/billing/product/fee%'  
	OR lower(e.event_type) =  '/event/activity/Rax/Fastlane'   
	OR lower(e.item_obj_type) = '/item/aws'   
	OR lower(e.service_type) = '/service/rax/fastlane/aws')   
	)   ;
	--*********************************************************************************************************************          
	-- CREATE INDEX IX_EVENT_POID_ID0 ON SSIS_Event_Load_Step2(EVENT_POID_ID0)    
	-- CREATE INDEX IX_EVENT_Item_Obj_Id0 ON SSIS_Event_Load_Step2(EVENT_Item_Obj_Id0)   
	-- CREATE INDEX IX_EVENT_ACCOUNT_POID ON SSIS_Event_Load_Step2(EVENT_ACCOUNT_POID)  
	-- CREATE INDEX IX_EVENT_TYPE ON SSIS_Event_Load_Step2(EVENT_TYPE)   
	--*********************************************************************************************************************  
	CREATE OR REPLACE TABLE stage_one.raw_ssis_impacts_load_step3_initial AS
SELECT * FROM stage_one.raw_ssis_impacts_load_step3_initial WHERE TRUE; 
	-- EXEC Drop_Indexes__SSIS_Impacts_Load_Step3_Initial           
	--*********************************************************************************************************************   
	INSERT INTO stage_one.raw_ssis_impacts_load_step3_initial        
	SELECT  
		product_poid_id0,         
		impactbal_event_obj_id0,          
		impact_category,       
		ebi_impact_type,     
		ebi_amount,  
		ebi_discount,  
		ebi_quantity,        
		ebi_rate_tag,       
		ebi_rec_id,       
		ebi_rum_id,         
		ebi_product_obj_id0,           
		ebi_product_obj_type,  
		ebi_currency_id,         
		ebi_gl_id,  
		CURRENT_DATETIME()      AS tbl_load_date,  
		ebi_offering_obj_id0,
    bill_created_date
	FROM  ( 
		-- OPENQUERY(EBI-ODS-CORE, '          
	SELECT          
		ebi.product_obj_id0       AS product_poid_id0,          
		ebi.obj_id0     		  AS impactbal_event_obj_id0,          
		ebi.impact_category    	  AS impact_category,          
		ebi.impact_type     	  AS ebi_impact_type,          
		ebi.amount      		  AS ebi_amount,   
		ebi.discount     		  AS ebi_discount,         
		ebi.quantity     		  AS ebi_quantity,          
		ebi.rate_tag     		  AS ebi_rate_tag,          
		ebi.rec_id      		  AS ebi_rec_id,          
		ebi.rum_id      		  AS ebi_rum_id,          
		ebi.product_obj_id0       AS ebi_product_obj_id0,           
		ebi.product_obj_type      AS ebi_product_obj_type,        
		ebi.resource_id     	  AS ebi_currency_id,            
		ebi.gl_id      			  AS ebi_gl_id,    
		ebi.offering_obj_id0      AS ebi_offering_obj_id0,
    E.Bill_Created_Date
	FROM  stage_one.raw_ssis_event_load_step2 e              
	INNER JOIN  `rax-landing-qa`.brm_ods.event_bal_impacts_t  ebi       
		ON  e.event_poid_id0 = ebi.obj_id0     
	WHERE ebi.resource_id  < 999 AND EBI.AMOUNT <> 0          
	)    ;
	--*********************************************************************************************************************          
	-- CREATE INDEX IX_IMPACTBAL_EVENT_OBJ_ID0 ON SSIS_Impacts_Load_Step3_Initial(IMPACTBAL_EVENT_OBJ_ID0)    
	-- CREATE INDEX IX_EBI_PRODUCT_OBJ_ID0 ON SSIS_Impacts_Load_Step3_Initial(EBI_PRODUCT_OBJ_ID0)   
	-- CREATE INDEX IX_EBI_REC_ID ON SSIS_Impacts_Load_Step3_Initial(EBI_REC_ID)   
	-- CREATE INDEX IX_EBI_RUM_ID ON SSIS_Impacts_Load_Step3_Initial(EBI_RUM_ID)   
	-- CREATE INDEX IX_EBI_AMOUNT ON SSIS_Impacts_Load_Step3_Initial(EBI_AMOUNT)   
	-- CREATE INDEX IX_IMPACT_CATEGORY ON SSIS_Impacts_Load_Step3_Initial(IMPACT_CATEGORY)   
	--*********************************************************************************************************************     
	CREATE OR REPLACE TABLE stage_one.raw_ssis_impacts_load_step3 AS
SELECT * FROM  stage_one.raw_ssis_impacts_load_step3 WHERE TRUE; 
	-- EXEC Drop_Indexes__SSIS_Impacts_Load_Step3     
	--*********************************************************************************************************************      
	INSERT INTO stage_one.raw_ssis_impacts_load_step3    
	SELECT          
		product_poid_id0,          
		prod_decsription,          
		product_name,          
		product_code,    
		impactbal_event_obj_id0,          
		impact_category,          
		ebi_impact_type,          
		ebi_amount,      
		ebi_discount,      
		ebi_quantity,          
		ebi_rate_tag,          
		ebi_rec_id,         
		ebi_rum_id,          
		ebi_product_obj_id0,           
		ebi_product_obj_type,          
		ebi_currency_id,           
		ebi_gl_id,           
		usage_record_id,          
		dc_id,          
		region_id,          
		res_id,          
		res_name,          
		managed_flag,          
		rum_name,        
		tax_rec_id,          
		tax_name,          
		tax_type_id,          
		tax_element_id,          
		tax_amount,          
		tax_rate_percent,     
		CURRENT_DATETIME()          AS tbl_load_date,  
		fastlane_impact_category,  
		fastlane_impact_value,  
		fastlane_impact_deal_code,  
		fastlane_impact_grp_code,  
		fastlane_impact_sub_grp_code,  
		COALESCE(CAST(fastlane_impact_is_backbill AS INT64),0)   AS fastlane_impact_is_backbill,  
		ebi_offering_obj_id0,
    bill_created_date
	FROM   (
		-- OPENQUERY(EBI-ODS-CORE, '                    
	SELECT          
		product_poid_id0,          
		COALESCE(prd.descr,disc.descr)  AS prod_decsription,          
		COALESCE(prd.name ,disc.name)  AS product_name,          
		COALESCE(prd.code ,disc.code)  AS product_code,    
		impactbal_event_obj_id0,          
		impact_category,          
		ebi_impact_type,          
		ebi_amount,      
		ebi_discount,      
		ebi_quantity,          
		ebi_rate_tag,          
		ebi_rec_id,         
		ebi_rum_id,          
		ebi_product_obj_id0,           
		ebi_product_obj_type,            
		ebi_currency_id,          
		ebi_gl_id,         
		edr.usage_record_id,          
		edr.dc_id,          
		edr.region_id,          
		edr.res_id,          
		edr.res_name,          
		edr.managed_flag,          
		erm.rum_name     AS rum_name,            
		etj.rec_id       AS tax_rec_id,          
		etj.name      	 AS tax_name,          
		etj.type         AS tax_type_id,          
		etj.element_id   AS tax_element_id,          
		etj.amount       AS tax_amount,          
		etj.percent      AS tax_rate_percent,    
		fastlane_invoicemap.impact_key   AS fastlane_impact_category,       
		fastlane_invoicemap.impact_value AS fastlane_impact_value,       
		deal_code        AS fastlane_impact_deal_code,            
		inv_grp_code     AS fastlane_impact_grp_code,          
		inv_sub_grp_code AS fastlane_impact_sub_grp_code,      
		backbill_flag    AS fastlane_impact_is_backbill,  
		ebi_offering_obj_id0,
    bill_created_date
	FROM stage_one.raw_ssis_impacts_load_step3_initial ebi            
	LEFT OUTER JOIN `rax-landing-qa`.brm_ods.product_t prd    
		ON ebi.product_poid_id0 = prd.poid_id0          
	LEFT OUTER JOIN `rax-landing-qa`.brm_ods.discount_t disc   
		ON ebi.product_poid_id0 = disc.poid_id0          
	LEFT OUTER JOIN `rax-landing-qa`.brm_ods.event_tax_jurisdictions_t etj   
		ON ebi.impactbal_event_obj_id0 = etj.obj_id0          
		AND ebi.ebi_rec_id = etj.element_id            
	LEFT OUTER JOIN `rax-landing-qa`.brm_ods.event_rum_map_t erm     
		ON ebi.impactbal_event_obj_id0 = erm.obj_id0          
		AND ebi.ebi_rum_id = erm.rec_id            
	LEFT OUTER JOIN `rax-landing-qa`.brm_ods.event_dlay_rax_t edr    
		ON ebi.impactbal_event_obj_id0 = edr.obj_id0             
	LEFT OUTER JOIN `rax-landing-qa`.brm_ods.config_fastlane_invoice_map_t fastlane_invoicemap   
		ON ebi.impact_category = fastlane_invoicemap.impact_code    
	LEFT OUTER JOIN `rax-landing-qa`.brm_ods.rax_fastlane_attributes_t fastlane   
		ON ebi.ebi_offering_obj_id0 = fastlane.offering_obj_id0        
	)     ;  
	--*********************************************************************************************************************          
	-- EXEC Create_Indexes__SSIS_Impacts_Load_Step3           
	--*********************************************************************************************************************       
END;
