CREATE OR REPLACE PROCEDURE `rax-staging-dev.stage_one.udsp_raw_cloud_bill_invoice_staging_audit`()
BEGIN

-------------------------------------------------------------------------------------------------------------------
declare runtime  string;
declare starttime  datetime;
set starttime = current_datetime();

insert into 
     `rax-staging-dev`.stage_one.raw_daily_bill_poid_audit_staging
select 
    bill_poid_id0,
    c.poid_id0		    as item_poid_id0,
    a.bill_no,
    a.ar_bill_obj_id0,
    account_poid_id0,
    cast(a.bill_start_date as date),
    cast(a.bill_end_date as date), 
    cast(a.bill_mod_date as date), 
    a.current_total,
    a.total_due,
    current_date()		    as load_date,
	a.bill_created_date 
from 
  `rax-staging-dev`.stage_one.raw_brm_invoice_aggregate_total a  
inner join
    `rax-staging-dev`.stage_one.raw_brm_invoice_line_item_audit b 
on a.bill_no=b.bill_no
inner join
    `rax-staging-dev`.stage_one.raw_cloud_brm_items c   
on a.bill_poid_id0=c.bill_obj_id0
where
   (a.bill_no not like '%ebs%' and a.bill_no not like '%evapt%');
   
insert into 
	 `rax-staging-dev`.stage_one.raw_cloud_bill_missing
   ( item_poid_id0,
    bill_poid_id0,
    bill_no,
    ar_bill_obj_id0,
    account_poid_id0,
    bill_start_date,
    bill_end_date, 
    bill_mod_date, 
    current_total,
    total_due,
	bill_created_date )
select 
    c.poid_id0		    as item_poid_id0,
    bill_poid_id0,
    a.bill_no,
    a.ar_bill_obj_id0,
    a.account_poid_id0,
    cast(a.bill_start_date as date) bill_start_date,
    cast( a.bill_end_date as date) as bill_end_date, 
    cast(  a.bill_mod_date  as date)  as bill_mod_date, 
    a.current_total,
    a.total_due,
	a.bill_created_date   
from 
    `rax-staging-dev`.stage_one.raw_brm_invoice_aggregate_total a  
inner join
    `rax-staging-dev`.stage_one.raw_cloud_brm_items c 
on a.bill_poid_id0=c.bill_obj_id0
where
    lower(a.bill_no) not like '%evapt%'
and not exists (select a.bill_no from stage_two_dw.stage_invitemeventdetail b  where a.bill_no = b.bill_no);


insert into
	`rax-staging-dev`.stage_one.raw_daily_bill_poid_audit_staging
select distinct 
    bill_poid_id0,
    item_poid_id0,
    a.bill_no,
    a.ar_bill_obj_id0,
    account_poid_id0,
    a.bill_start_date,
    a.bill_end_date, 
    a.bill_mod_date, 
    a.current_total,
    a.total_due,
    current_datetime()		as load_date,
	a.bill_created_date
from 
   `rax-staging-dev`.stage_one.raw_cloud_bill_missing a 
where
     not exists (select item_poid_id0 from  `rax-staging-dev`.stage_one.raw_daily_bill_poid_audit_staging b  where a.item_poid_id0 = b.item_poid_id0) ;


CREATE OR REPLACE TABLE  `rax-staging-dev.stage_one.raw_ssis_invoice_audit_load_step1`  AS
select * from `rax-staging-dev.stage_one.raw_ssis_invoice_audit_load_step1`where true;
--exec [drop_indexes__raw_ssis_invoice_audit_load_step1]

insert into
    `rax-staging-dev.stage_one.raw_ssis_invoice_audit_load_step1`
select  
    account_poid_id0,
    account_no										    as brm_accountno,
    substring(account_no, strpos(account_no,'-')+1,64)	    as account_id,
    gl_segment										    as gl_segment,
    bill_poid_id0,
    bill_no,
    bill_start_date,
    bill_end_date,
    bill_mod_date,
    current_total,
    total_due,
    item_poid_id0,
    --cast(date_add(ss,effective_t, '1970-01-01') as date)	    as item_effective_date ,
     DATE(TIMESTAMP_SECONDS(cast(effective_t as INT64))) AS item_effective_date, 
      DATE(TIMESTAMP_SECONDS(cast(item_mod_t as INT64))) AS item_mod_date, 
    --cast(date_add(ss,item_mod_t, '1970-01-01') as date)		    as item_mod_date ,
    name											    as item_name,
    status										    as item_status,
    service_obj_type								    as service_obj_type,
    poid_type										    as item_type,
    current_datetime()										    as tbl_load_date,
	bill_created_date
from (
select distinct
    a.poid_id0										    as account_poid_id0,
    a.account_no,
    a.gl_segment,
    bill_poid_id0,
    bill_no,
    bill_start_date,
    bill_end_date, 
    bill_mod_date, 
    current_total,
    total_due,
    i.poid_id0										    as item_poid_id0,
    i.effective_t,
    i.mod_t										    as item_mod_t,
    i.name,
    i.status,
    i.service_obj_type,
    i.poid_type,
	bill_created_date
from 
   stage_one.raw_daily_bill_poid_audit_staging spoid
inner join  
    `rax-staging-dev.stage_one.raw_brm_item_t`  i 
on  spoid.item_poid_id0=i.poid_id0 
inner join
     `rax-landing-qa.brm_ods.account_t` a 
on i.account_obj_id0= a.poid_id0 
where 
     lower(a.gl_segment) LIKE '%cloud%')
;
--*********************************************************************************************************************
--*********************************************************************************************************************
--set runtime = convert(STRING,current_datetime()-starttime,8);
------raiserror ('2 complete (raw_ssis_invoice_audit_load_step1 is loaded):  %s',10,1,@runtime) with nowait
--*********************************************************************************************************************
CREATE OR REPLACE TABLE  `rax-staging-dev.stage_one.raw_ssis_event_audit_load_step2_initial`  AS
select * from `rax-staging-dev.stage_one.raw_ssis_event_audit_load_step2_initial`where true;

--truncate table raw_ssis_event_audit_load_step2_initial;
----exec drop_indexes__raw_ssis_event_audit_load_step2_initial;
--********************************************************************************************************************* 
---event_t
insert into
 `rax-staging-dev.stage_one.raw_ssis_event_audit_load_step2_initial`
 select 
    event_poid_id0,
    item_obj_id0,
    event_account_poid_id0,			 
    start_t,
    end_t, 
    mod_t, 
    service_obj_type,
    item_obj_type,
    poid_type,
    rerate_obj_id0,
    batch_id,
    name,
    sys_descr,
    rum_name,
    created_t,
    current_datetime()			   as  tbl_load_date,
    earned_start_t,	
    earned_end_t,
	service_obj_id0,
	bill_created_date
from
(select distinct
    e.poid_id0						as event_poid_id0,
    e.item_obj_id0,
    e.account_obj_id0				as event_account_poid_id0,			 
    e.start_t,
    e.end_t, 
    e.mod_t, 
    e.service_obj_type,
    e.item_obj_type,
    e.poid_type,    
    e.rerate_obj_id0,
    e.batch_id,
    e.name,
    e.sys_descr,
    e.rum_name,
    e.created_t,
    e.earned_start_t,	
    e.earned_end_t,
	e.service_obj_id0,
	a.bill_created_date
from 
  stage_one.raw_ssis_invoice_audit_load_step1 a 
inner join
    `rax-landing-qa.brm_ods.event_t` e     
on a.item_poid_id0=e.item_obj_id0 
) ;
--*********************************************************************************************************************

--*********************************************************************************************************************
--set runtime = convert(STRING,current_datetime()-starttime,8);
---raiserror ('3 complete raw_ssis_event_audit_load_step2_initial):  %s',10,1,@runtime) with nowait
--*********************************************************************************************************************
CREATE OR REPLACE TABLE  `rax-staging-dev.stage_one.raw_ssis_event_archive_audit_load_step2_initial`  AS
select * from `rax-staging-dev.stage_one.raw_ssis_event_archive_audit_load_step2_initial`where true;

--truncate table dbo.raw_ssis_event_archive_audit_load_step2_initial;
---exec drop_indexes__raw_ssis_event_archive_audit_load_step2_initial;
--********************************************************************************************************************* 
insert into
   `rax-staging-dev.stage_one.raw_ssis_event_archive_audit_load_step2_initial` 
select
     event_poid_id0,
    item_obj_id0,
    event_account_poid_id0,			 
    start_t,
    end_t, 
    mod_t, 
    service_obj_type,
    item_obj_type,
    poid_type,
    rerate_obj_id0,
    batch_id,
    name,
    sys_descr,
    rum_name,
    created_t,
    current_datetime()			   as  tbl_load_date,
    earned_start_t,	
    earned_end_t,
	service_obj_id0,
	bill_created_date
from
 (select 
    e.poid_id0						as event_poid_id0,
    e.item_obj_id0,
    e.account_obj_id0				as event_account_poid_id0,			 
    e.start_t,
    e.end_t, 
    e.mod_t, 
    e.service_obj_type,
    e.item_obj_type,
    e.poid_type,    
    e.rerate_obj_id0,
    e.batch_id,
    e.name,
    e.sys_descr,
    e.rum_name,
    e.created_t,
    e.earned_start_t,	
    e.earned_end_t,
	e.service_obj_id0,
	a.bill_created_date
from 
  stage_one.raw_ssis_invoice_audit_load_step1 a 
inner join
    `rax-landing-qa.brm_ods.event_t_archive` e    
on a.item_poid_id0=e.item_obj_id0
) ;

--*********************************************************************************************************************
insert into
    `rax-staging-dev.stage_one.raw_ssis_event_archive_audit_load_step2_initial`
select
    event_poid_id0,
    item_obj_id0,
    event_account_poid_id0,			 
    start_t,
    end_t, 
    mod_t, 
    service_obj_type,
    item_obj_type,
    poid_type,
    rerate_obj_id0,
    batch_id,
    name,
    sys_descr,
    rum_name,
    created_t,
    current_datetime(),
    earned_start_t,	
    earned_end_t,
	service_obj_id0,
	bill_created_date
from
      `rax-staging-dev.stage_one.raw_ssis_event_archive_audit_load_step2_initial` a 
where
    not exists (select event_poid_id0 from  `rax-staging-dev.stage_one.raw_ssis_event_archive_audit_load_step2_initial`  xx    where xx.event_poid_id0=a.event_poid_id0);
--*********************************************************************************************************************
--set runtime = convert(STRING,current_datetime()-starttime,8);
---raiserror ('4 complete raw_ssis_event_archive_audit_load_step2_initial):  %s',10,1,@runtime) with nowait
--*********************************************************************************************************************

CREATE OR REPLACE TABLE  `rax-staging-dev.stage_one.raw_ssis_event_unpartitioned_audit_load_step2_initial`  AS
select * from `rax-staging-dev.stage_one.raw_ssis_event_unpartitioned_audit_load_step2_initial` where true;

--truncate table raw_ssis_event_unpartitioned_audit_load_step2_initial;
----exec drop_indexes__raw_ssis_event_unpartitioned_audit_load_step2_initial
--********************************************************************************************************************* 
insert into
`rax-staging-dev.stage_one.raw_ssis_event_unpartitioned_audit_load_step2_initial`
select   
    event_poid_id0,
    item_obj_id0,
    event_account_poid_id0,			 
    start_t,
    end_t, 
    mod_t, 
    service_obj_type,
    item_obj_type,
    poid_type,
    rerate_obj_id0,
    batch_id,
    name,
    sys_descr,
    rum_name,
    created_t,
   current_datetime()		   as  tbl_load_date,
    earned_start_t,	
    earned_end_t,
	service_obj_id0,
	bill_created_date
from
    (select  
     e.poid_id0						as event_poid_id0,
    e.item_obj_id0,
    e.account_obj_id0				as event_account_poid_id0,			 
    e.start_t,
    e.end_t, 
    e.mod_t, 
    e.service_obj_type,
    e.item_obj_type,
    e.poid_type,    
    e.rerate_obj_id0,
    e.batch_id,
    e.name,
    e.sys_descr,
    e.rum_name,
    e.created_t,
    e.earned_start_t,	
    e.earned_end_t,
	e.service_obj_id0,
	a.bill_created_date
from 
   stage_one.raw_ssis_invoice_audit_load_step1 a  
inner join
    `rax-landing-qa.brm_ods.event_t__unpartitioned_archive` e   
on a.item_poid_id0=e.item_obj_id0
) ;

--*********************************************************************************************************************
insert into
    `rax-staging-dev.stage_one.raw_ssis_event_audit_load_step2_initial`
select
    event_poid_id0,
    item_obj_id0,
    event_account_poid_id0,			 
    start_t,
    end_t, 
    mod_t, 
    service_obj_type,
    item_obj_type,
    poid_type,
    rerate_obj_id0,
    batch_id,
    name,
    sys_descr,
    rum_name,
    created_t,
    current_datetime()			   as  tbl_load_date,
    earned_start_t,	
    earned_end_t,
	service_obj_id0,
	bill_created_date
from
      `rax-staging-dev.stage_one.raw_ssis_event_audit_load_step2_initial` a  
where
    not exists (select event_poid_id0 from  `rax-staging-dev.stage_one.raw_ssis_event_audit_load_step2_initial`  xx    where xx.event_poid_id0=a.event_poid_id0);

--*********************************************************************************************************************
--set runtime = convert(STRING,current_datetime()-starttime,8);
-----raiserror ('5 complete raw_ssis_event_unpartitioned_audit_load_step2_initial):  %s',10,1,runtime) ;
--********************************************************************************************************************* 
create or replace temporary table  temp_account_t  AS
select 
    *
from
`rax-landing-qa.brm_ods.account_t` a 
 ;

--*********************************************************************************************************************
create or replace temporary table 
    temp_event_act_rax_fastlane_t AS
   select 
   obj_id0, duration, attr1, attr2, attr3, data_center_id, inv_grp_code, inv_sub_grp_code, record_id, region, resource_id, resource_name, dw_timestamp, attr4, attr5, attr6, uom, currency_rate, backbill_flag as activity_is_backbill
from
    `rax-landing-qa.brm_ods.event_act_rax_fastlane_t` as fastlane;

--*****************************************************************;****************************************************
CREATE OR REPLACE TABLE  `rax-staging-dev.stage_one.raw_ssis_event_audit_load_step2`  AS
select * from `rax-staging-dev.stage_one.raw_ssis_event_audit_load_step2` where true;

--truncate table raw_ssis_event_audit_load_step2;
----exec [drop_indexes_raw_ssis_event_audit_load_step2]
--********************************************************************************************************************* 
---event_t
insert into
   `rax-staging-dev.stage_one.raw_ssis_event_audit_load_step2`   
select
    event_poid_id0,
    event_account_poid_id0,
    item_obj_id0										as event_item_obj_id0,
    --cast(dateadd(ss,e.start_t, '1970-01-01') as date)	as event_start_date,
	     DATE(TIMESTAMP_SECONDS(cast(e.start_t as INT64))) AS event_start_date, 
	     DATE(TIMESTAMP_SECONDS(cast(e.end_t as INT64))) AS event_end_date, 
	     DATE(TIMESTAMP_SECONDS(cast(e.mod_t as INT64))) AS event_mod_date, 

    --cast( dateadd(ss,e.end_t, '1970-01-01') as date)	as event_end_date,
--cast( dateadd(ss,e.mod_t, '1970-01-01') as date)	as event_mod_date,
    service_obj_type									as service_type,
    e.poid_type											as event_type,
    rerate_obj_id0,
    batch_id,
    e.name												as event_name,
    sys_descr											as event_sys_descr,
    rum_name											as event_rum_name,
    --cast(dateadd(ss,e.created_t, '1970-01-01') as date)	as event_created_date,
		     DATE(TIMESTAMP_SECONDS(cast(e.created_t as INT64))) AS event_created_date, 

    inv_grp_code										as activity_service_type,   --new ield added 1.20.2017 kvc
    inv_sub_grp_code									as activity_event_type,		--new ield added 1.20.2017 kvc
    record_id											as activity_record_id,		--new ield added 1.20.2017 kvc
    data_center_id										as activity_dc_id,			--new ield added 1.20.2017 kvc
    region										as activity_region,			--new ield added 1.20.2017 kvc
    resource_id									as activity_resource_id,	--new ield added 1.20.2017 kvc
    resource_name									as activity_resource_name,  --new ield added 1.20.2017 kvc
    attr1											as activity_attr1,			--new ield added 1.20.2017 kvc
    attr2											as activity_attr2,			--new ield added 1.20.2017 kvc
    attr3											as activity_attr3,			--new ield added 1.20.2017 kvc
    current_datetime()										as tbl_load_date,
    --cast(dateadd(ss,earned_start_t,'1970-01-01') as date)	as event_earned_start_date,
    		     DATE(TIMESTAMP_SECONDS(cast(earned_start_t as INT64))) AS event_earned_start_date, 
    		     DATE(TIMESTAMP_SECONDS(cast(earned_end_t as INT64))) AS event_earned_end_date, 

	--cast(dateadd(ss,earned_end_t,'1970-01-01') as date)	as event_earned_end_date,
    cast(IFNULL(activity_is_backbill,0)	as INT64)					as activity_is_backbill,
	attr4											as activity_attr4,
	attr5											as activity_attr5,
	attr6											as activity_attr6,
	service_obj_id0,
	bill_created_date
from
    `rax-staging-dev.stage_one.raw_ssis_event_audit_load_step2_initial` e   
inner join
	temp_account_t acct 
on e.event_account_poid_id0=acct.poid_id0  
left outer join    
    temp_event_act_rax_fastlane_t as fastlane 
on e.event_poid_id0 = fastlane.obj_id0 
where
	lower(acct.gl_segment) like ('%cloud%')
and (e.batch_id is null  or lower(e.batch_id)	not like 'rerating%')	
and (lower(e.poid_type) like '/event/delayed/rax/cloud/%'
or lower(e.poid_type)= '/event/billing/cycle/discount'
or lower(e.poid_type)='/event/billing/cycle/tax'
or lower(e.poid_type)= '/event/billing/cycle/fold'
or lower(e.poid_type) like '%/event/billing/product/fee%'
or lower(e.poid_type) =  '/event/activity/rax/fastlane' 
or lower(e.item_obj_type) = '/item/aws' 
or lower(e.service_obj_type) = '/service/rax/fastlane/aws') ;

--*********************************************************************************************************************
--set runtime = convert(STRING,current_datetime()-starttime,8);
---raiserror ('6 complete raw_ssis_event_audit_load_step2):  %s',10,1,@runtime) with nowait
--*********************************************************************************************************************
CREATE OR REPLACE TABLE  `rax-staging-dev.stage_one.raw_impacts_audit_load_step1`  AS
select * from `rax-staging-dev.stage_one.raw_impacts_audit_load_step1` where true;

--truncate table raw_impacts_audit_load_step1;
-----exec drop_indexes_raw_impacts_audit_load_step1
--*********************************************************************************************************************
------raw_impacts_audit_load_step2 using event_bal_impacts_t
insert into
	`rax-staging-dev.stage_one.raw_impacts_audit_load_step1`
select distinct
    impactbal_event_obj_id0,
    impact_category,
    ebi_item_obj_id0,
    ebi_impact_type,
    ebi_amount,
    ebi_quantity,
    ebi_rate_tag,
    ebi_rec_id,
    ebi_rum_id,
    ebi_product_obj_id0,
    ebi_product_obj_type,		 -- new field added 5.24.16_kvc
    ebi_currency_id,			 -- new field added 12.07.15_jcm
    ebi_gl_id,					 -- new field added 5.24.16_kvc
    current_datetime()					as tbl_load_date,
    ebi_offering_obj_id0
from 
(select 
    ebi.obj_id0										   as impactbal_event_obj_id0,
    ebi.item_obj_id0									   as ebi_item_obj_id0,
    ebi.impact_category									   as impact_category,
    ebi.impact_type										   as ebi_impact_type,
    ebi.amount											   as ebi_amount,
    ebi.quantityx										   as ebi_quantity,
    ebi.rate_tag										   as ebi_rate_tag,
    ebi.rec_id											   as ebi_rec_id,
    ebi.rum_id											   as ebi_rum_id,
    ebi.product_obj_id0								   as ebi_product_obj_id0,
    ebi.product_obj_type									   as ebi_product_obj_type,		-- new field added 5.24.16_kvc
    ebi.resource_id  									   as ebi_currency_id,				-- new field added 12.07.15_jcm
    ebi.gl_id				 							   as ebi_gl_id,						-- new field added 5.24.16_kvc
    ebi.offering_obj_id0									   as ebi_offering_obj_id0	-- new field added 5.24.16_kvc       
from
   stage_one.raw_ssis_event_audit_load_step2 e 
inner join
    `rax-staging-dev.stage_one.stg_event_bal_impacts_t`  ebi  
on e.event_poid_id0=ebi.obj_id0  
and e.event_account_poid_id0=account_obj_id0
where
    ebi.resource_id  < 999 
and IFNULL(ebi.amount,0)<> 0
) ;

--*********************************************************************************************************************
--set runtime = convert(STRING,current_datetime()-starttime,8);
---raiserror ('7 complete (raw_impacts_audit_load_step1 event_bal_impacts_t is loaded):  %s',10,1,@runtime) with nowait
--********************************************************************************************************************* 

CREATE OR REPLACE TABLE  `rax-staging-dev.stage_one.raw_impacts_audit_load_step2`  AS
select * from `rax-staging-dev.stage_one.raw_impacts_audit_load_step2` where true;
--truncate table  raw_impacts_audit_load_step2;
---exec [drop_indexes_raw_impacts_audit_load_step2]
--*********************************************************************************************************************
------raw_impacts_audit_load_step2 using event_bal_impacts_t_archive
insert into
    `rax-staging-dev.stage_one.raw_impacts_audit_load_step2`
select distinct
    impactbal_event_obj_id0,
    impact_category,
    ebi_item_obj_id0,
    ebi_impact_type,
    ebi_amount,
    ebi_quantity,
    ebi_rate_tag,
    ebi_rec_id,
    ebi_rum_id,
    ebi_product_obj_id0,
    ebi_product_obj_type,	-- new field added 5.24.16_kvc												
    ebi_currency_id,		-- new field added 12.07.15_jcm
    ebi_gl_id,	 			-- new field added 5.24.16_kvc
    current_datetime()			   as tbl_load_date,
    ebi_offering_obj_id0
from 
(select distinct
    ebi.obj_id0										   as impactbal_event_obj_id0,
    ebi.item_obj_id0									   as ebi_item_obj_id0,
    ebi.impact_category									   as impact_category,
    ebi.impact_type										   as ebi_impact_type,
    ebi.amount											   as ebi_amount,
    ebi.quantity										   as ebi_quantity,
    ebi.rate_tag										   as ebi_rate_tag,
    ebi.rec_id											   as ebi_rec_id,
    ebi.rum_id											   as ebi_rum_id,
    ebi.product_obj_id0								   as ebi_product_obj_id0,
    ebi.product_obj_type									   as ebi_product_obj_type,		-- new field added 5.24.16_kvc												as ebi_product_obj_type, -- new field added 5.24.16_kvc
    ebi.resource_id  									   as ebi_currency_id,			-- new field added 12.07.15_jcm
    ebi.gl_id				 							   as ebi_gl_id,				-- new field added 5.24.16_kvc
    ebi.offering_obj_id0									   as ebi_offering_obj_id0	-- new field added 5.24.16_kvc       

from
    stage_one.raw_ssis_event_audit_load_step2 e 
inner join
   `rax-landing-qa.brm_ods.event_bal_impacts_t_archive`  ebi  
on e.event_poid_id0=ebi.obj_id0  
and e.event_account_poid_id0=account_obj_id0
where
   ebi.resource_id  < 999 
and IFNULL(ebi.amount,0)<> 0
) ;

--********************************************************************************************************************* 
insert into
    `rax-staging-dev.stage_one.raw_impacts_audit_load_step1`
select distinct
    impactbal_event_obj_id0,
    impact_category,
    ebi_item_obj_id0,
    ebi_impact_type,
    ebi_amount,
    ebi_quantity,
    ebi_rate_tag,
    ebi_rec_id,
    ebi_rum_id,
    ebi_product_obj_id0,
    ebi_product_obj_type,	-- new field added 5.24.16_kvc												
    ebi_currency_id,		-- new field added 12.07.15_jcm
    ebi_gl_id,	 			-- new field added 5.24.16_kvc
    current_datetime()			   as tbl_load_date,
    ebi_offering_obj_id0
from
    `rax-staging-dev.stage_one.raw_impacts_audit_load_step2` a  
where
    not exists (select impactbal_event_obj_id0 from  `rax-staging-dev.stage_one.raw_impacts_audit_load_step1`  xx   where xx.impactbal_event_obj_id0=a.impactbal_event_obj_id0);
--*********************************************************************************************************************
--set runtime = convert(STRING,current_datetime()-starttime,8);
---raiserror ('7 complete (raw_impacts_audit_load_step2 event_bal_impacts_t_archive is loaded):  %s',10,1,@runtime) with nowait
--*********************************************************************************************************************

CREATE OR REPLACE TABLE  `rax-staging-dev.stage_one.raw_impacts_audit_load_step3`  AS
select * from `rax-staging-dev.stage_one.raw_impacts_audit_load_step3` where true;
--truncate table raw_impacts_audit_load_step3;
---exec [drop_indexes_raw_impacts_audit_load_step3]
--*********************************************************************************************************************
------raw_impacts_audit_load_step2 using event_bal_impacts_t_archive
insert into
    `rax-staging-dev.stage_one.raw_impacts_audit_load_step3`
select distinct
    impactbal_event_obj_id0,
    impact_category,
    ebi_item_obj_id0,
    ebi_impact_type,
    ebi_amount,
    ebi_quantity,
    ebi_rate_tag,
    ebi_rec_id,
    ebi_rum_id,
    ebi_product_obj_id0,
    ebi_product_obj_type,	-- new field added 5.24.16_kvc												
    ebi_currency_id,		-- new field added 12.07.15_jcm
    ebi_gl_id,	 			-- new field added 5.24.16_kvc
    current_datetime()			   as tbl_load_date,
    ebi_offering_obj_id0
from 
(select distinct
    ebi.obj_id0										   as impactbal_event_obj_id0,
    ebi.item_obj_id0									   as ebi_item_obj_id0,
    ebi.impact_category									   as impact_category,
    ebi.impact_type										   as ebi_impact_type,
    ebi.amount											   as ebi_amount,
    ebi.quantity										   as ebi_quantity,
    ebi.rate_tag										   as ebi_rate_tag,
    ebi.rec_id											   as ebi_rec_id,
    ebi.rum_id											   as ebi_rum_id,
    ebi.product_obj_id0								   as ebi_product_obj_id0,
    ebi.product_obj_type									   as ebi_product_obj_type,		-- new field added 5.24.16_kvc												as ebi_product_obj_type, -- new field added 5.24.16_kvc
    ebi.resource_id  									   as ebi_currency_id,			-- new field added 12.07.15_jcm
    ebi.gl_id				 							   as ebi_gl_id,				-- new field added 5.24.16_kvc
    ebi.offering_obj_id0									   as ebi_offering_obj_id0	-- new field added 5.24.16_kvc       
from
    `rax-staging-dev.stage_one.raw_ssis_event_audit_load_step2` e 
inner join
   `rax-landing-qa.brm_ods.event_bal_impacts_t__unpartitioned_archive`  ebi  
on e.event_poid_id0=ebi.obj_id0  
and e.event_account_poid_id0=account_obj_id0
where
   ebi.resource_id  < 999 
and IFNULL(ebi.amount,0)<> 0
); 

--*********************************************************************************************************************
insert into
    `rax-staging-dev.stage_one.raw_impacts_audit_load_step1`
select distinct
    impactbal_event_obj_id0,
    impact_category,
    ebi_item_obj_id0,
    ebi_impact_type,
    ebi_amount,
    ebi_quantity,
    ebi_rate_tag,
    ebi_rec_id,
    ebi_rum_id,
    ebi_product_obj_id0,
    ebi_product_obj_type,	-- new field added 5.24.16_kvc												
    ebi_currency_id,		-- new field added 12.07.15_jcm
    ebi_gl_id,	 			-- new field added 5.24.16_kvc
    current_datetime()			   as tbl_load_date,
    ebi_offering_obj_id0
from
    `rax-staging-dev.stage_one.raw_impacts_audit_load_step3` a  
where
    not exists (select impactbal_event_obj_id0 from  `rax-staging-dev.stage_one.raw_impacts_audit_load_step1`  xx    where xx.impactbal_event_obj_id0=a.impactbal_event_obj_id0);
--*********************************************************************************************************************
--set runtime = convert(STRING,current_datetime()-starttime,8);
--raiserror ('8 complete (raw_impacts_audit_load_step3 event_bal_impacts_t__unpartitioned_archive is loaded):  %s',10,1,@runtime) with nowait
--*********************************************************************************************************************
CREATE OR REPLACE TABLE  `rax-staging-dev.stage_one.raw_ssis_impacts_audit_load_step3`  AS
select * from `rax-staging-dev.stage_one.raw_ssis_impacts_audit_load_step3` where true;

--truncate table dbo.raw_ssis_impacts_audit_load_step3;
--exec [drop_indexes_raw_ssis_impacts_audit_load_step3]
--*********************************************************************************************************************
insert into 
    `rax-staging-dev.stage_one.raw_ssis_impacts_audit_load_step3`
select distinct
    product_poid_id0,
    prod_decsription,
    product_name,
    product_code,
    impactbal_event_obj_id0,
    impact_category,
    ebi_impact_type,
    ebi_amount,
    ebi_quantity,
    ebi_rate_tag,
    ebi_rec_id,
    ebi_rum_id,
    ebi_product_obj_id0,
    ebi_product_obj_type,		-- new field added 5.24.16_kvc												
    ebi_currency_id,			-- new field added 12.07.15_jcm
    ebi_gl_id,				-- new field added 5.24.16_kvc
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
    fastlane_impact_category,	--new ield added 1.20.2017 kvc
    fastlane_impact_value,		--new ield added 1.20.2017 kvc
    current_datetime()											   as tbl_load_date,
    fastlane_impact_deal_code,
    fastlane_impact_grp_code,
    fastlane_impact_sub_grp_code,
    cast(IFNULL(fastlane_impact_is_backbill,0) as INT64)				   as fastlane_impact_is_backbill,
    ebi_offering_obj_id0
from 
(select distinct
    ebi_product_obj_id0									   as product_poid_id0,
    IFNULL(prd.descr,disc.descr)							   as prod_decsription,
    IFNULL(prd.name	,disc.name)							   as product_name,
    IFNULL(prd.code	,disc.code)							   as product_code,
    impactbal_event_obj_id0,
    impact_category,
    ebi_impact_type,
    ebi_amount,
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
    erm.rum_name 										   as rum_name,  
    etj.rec_id											   as tax_rec_id,
    etj.name											   as tax_name,
    etj.type											   as tax_type_id,
    etj.element_id										   as tax_element_id,
    etj.amount											   as tax_amount,
    etj.percent										   as tax_rate_percent,
    fastlane_invoicemap.impact_key							   as fastlane_impact_category,	--new ield added 1.20.2017 kvc
    fastlane_invoicemap.impact_value						   as fastlane_impact_value,		--new ield added 1.20.2017 kvc
    deal_code											   as fastlane_impact_deal_code,	    --new ield added 5.1.2019 kvc     
    inv_grp_code										   as fastlane_impact_grp_code,	    --new ield added 5.1.2019 kvc     
    inv_sub_grp_code									   as fastlane_impact_sub_grp_code,   --new ield added 5.1.2019 kvc     
    backbill_flag										   as fastlane_impact_is_backbill,  --new ield added 5.1.2019 kvc 
    ebi_offering_obj_id0       
from
     `rax-staging-dev.stage_one.raw_impacts_audit_load_step1` ebi 
left outer join
    `rax-landing-qa.brm_ods.product_t` prd 
on ebi_product_obj_id0= prd.poid_id0
left outer join
     `rax-landing-qa.brm_ods.discount_t` disc 
on ebi_product_obj_id0= disc.poid_id0
left outer join
     `rax-landing-qa.brm_ods.event_tax_jurisdictions_t` etj	
on impactbal_event_obj_id0= etj.obj_id0 --(+)
and ebi_rec_id=etj.element_id --(+)
left outer join
`rax-landing-qa.brm_ods.event_rum_map_t` erm	
on impactbal_event_obj_id0=erm.obj_id0
and ebi_rum_id=erm.rec_id 
left outer join
`rax-landing-qa.brm_ods.event_dlay_rax_t` edr	
on impactbal_event_obj_id0=edr.obj_id0
left outer join
`rax-landing-qa.brm_ods.config_fastlane_invoice_map_t` fastlane_invoicemap	
on impact_category=fastlane_invoicemap.impact_code    
left outer join
`rax-landing-qa.brm_ods.rax_fastlane_attributes_t` fastlane  
on ebi.ebi_offering_obj_id0=fastlane.offering_obj_id0  
) ;

--*********************************************************************************************************************
--set runtime = convert(STRING,current_datetime()-starttime,8);
----raiserror ('9 complete raw_ssis_impacts_audit_load_step3 event_dlay_rax_t is loaded):  %s',10,1,@runtime) with nowait
--*********************************************************************************************************************
create or replace temporary table temp_event_dlay_rax_t_archive_update
              as
    
select distinct
    obj_id0,
    usage_record_id,
    dc_id,
    region_id,
    res_id,
    res_name,
    managed_flag   
from 
(select distinct 
    edr.usage_record_id,
    edr.obj_id0,
    edr.dc_id,
    edr.region_id,
    edr.res_id,
    edr.res_name,
    edr.managed_flag
from
     `rax-staging-dev.stage_one.raw_ssis_impacts_audit_load_step3` ebi 
inner join
    `rax-landing-qa.brm_ods.event_dlay_rax_t_archive` edr 
on impactbal_event_obj_id0=edr.obj_id0
) ;

--*********************************************************************************************************************
--set runtime = convert(STRING,current_datetime()-starttime,8);
-----raiserror ('10 complete create #event_dlay_rax_t_archive_update is loaded):  %s',10,1,@runtime) with nowait
--*********************************************************************************************************************
update
`rax-staging-dev.stage_one.raw_ssis_impacts_audit_load_step3` a 
set
    a.usage_record_id=b.usage_record_id,
    a.dc_id=b.dc_id,
    a.region_id=b.region_id,
    a.res_id=b.res_id,
    a.res_name=b.res_name,
    a.managed_flag =b.managed_flag 
from
     temp_event_dlay_rax_t_archive_update b
where a.impactbal_event_obj_id0=b.obj_id0;
--*********************************************************************************************************************
--set runtime = convert(STRING,current_datetime()-starttime,8);
--raiserror ('11 complete update raw_ssis_impacts_audit_load_step3 from #event_dlay_rax_t_archive_update is loaded):  %s',10,1,@runtime) with nowait
--*********************************************************************************************************************
create or replace temporary table    temp_event_dlay_rax_t__unpartitioned_archive_update 

              as
select distinct
    obj_id0,
    usage_record_id,
    dc_id,
    region_id,
    res_id,
    res_name,
    managed_flag   
from 
(select distinct 
    edr.usage_record_id,
    edr.obj_id0,
    edr.dc_id,
    edr.region_id,
    edr.res_id,
    edr.res_name,
    edr.managed_flag
from
    `rax-staging-dev.stage_one.raw_ssis_impacts_audit_load_step3` ebi 
inner join
    `rax-landing-qa.brm_ods.event_dlay_rax_t__unpartitioned_archive`  edr	
on impactbal_event_obj_id0=edr.obj_id0
) ;

--*********************************************************************************************************************
--set runtime = convert(STRING,current_datetime()-starttime,8);
----raiserror ('12 complete create #event_dlay_rax_t__unpartitioned_archive_update is loaded):  %s',10,1,@runtime) with nowait
--*********************************************************************************************************************
update
`rax-staging-dev.stage_one.raw_ssis_impacts_audit_load_step3` a
set
    a.usage_record_id=b.usage_record_id,
    a.dc_id=b.dc_id,
    a.region_id=b.region_id,
    a.res_id=b.res_id,
    a.res_name=b.res_name,
    a.managed_flag =b.managed_flag 
from
     temp_event_dlay_rax_t__unpartitioned_archive_update b
where a.impactbal_event_obj_id0=b.obj_id0;
--*********************************************************************************************************************
--set runtime = convert(STRING,current_datetime()-starttime,8);
---raiserror ('13 complete update raw_ssis_impacts_audit_load_step3 from ##event_dlay_rax_t__unpartitioned_archive_update is loaded):  %s',10,1,@runtime) with nowait


END;
