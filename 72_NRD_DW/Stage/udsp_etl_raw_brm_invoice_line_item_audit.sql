CREATE OR REPLACE PROCEDURE `rax-staging-dev.stage_one.udsp_etl_raw_brm_invoice_line_item_audit`()
BEGIN


----------------------------------------------------------------------------------------------------------
-------------------------------------------------------------------------------------------------------------
declare hmdb_end datetime;
declare evapt_end  datetime;
-------------------------------------------------------------------------------------------------------------
set hmdb_end ='2015-2-2';
set evapt_end ='2016-1-7';
-------------------------------------------------------------------------------------------------------------

-------------------------------------------------------------------------------------------
create or replace temporary table temp_brm_source_total
as
select distinct  
    bill_no,   
    gl_segment,
    bill_end_date,
    sum(total_due)					    as total_due
from
       (select distinct  
    account_id,
    gl_segment,
    company_name,
    bill_poid_id0					    as bill_poid,
    bill_no,
    bill_end_date,
    bill_mod_date,
    cast(current_total as INT64)    as total_due
--into    #brm_source_total_stage
from
    stage_one.raw_brm_invoice_aggregate_total a  
where
 (current_total)<>0  
and  (lower(bill_no) not like '%ebs%' and lower(bill_no) not like '%evapt%') 
and (lower(gl_segment) not like '%.cloud.us%'  and bill_end_date < hmdb_end)
and (lower(gl_segment) not like '%.cloud.uk%'  and bill_end_date < evapt_end) )a -----#brm_source_total_stage a with(nolock) 
group by
     bill_no,
     gl_segment,
     bill_end_date;
-------------------------------------------------------------------------------------------
create or replace temporary table temp_brm_invoice_line_item_all
as
select
    bill_no						    as invoice,
    gl_segment,
    item_name,
    cast(sum(amount) as INT64)  as item_price
from
    stage_two_dw.stage_invitemeventdetail 
where
   bill_end_date >=(cast(date_trunc(date_sub(current_date(), interval 4 year), month) as datetime))
and bill_end_date <= (current_datetime())
group by
    bill_no,
    gl_segment,
    item_name;


-------------------------------------------------------------------------------------------
create or replace temporary table temp_brm_invoice_line_item_tax
as
select
    invoice,
    gl_segment,
    sum(item_price)					    as line_item_total_tax
from
    temp_brm_invoice_line_item_all 
where
    lower(item_name) like '%tax%'
group by
	invoice,
	gl_segment;

-------------------------------------------------------------------------------------------
create or replace temporary table temp_brm_invoice_line_item
as
select
    invoice,
    gl_segment,
    sum(item_price)			  as line_item_total,
    cast(0 as INT64)	  as line_item_total_tax
from
    temp_brm_invoice_line_item_all
group by
    invoice,
    gl_segment;

------------------------------------------------------------------------------------------
update
   temp_brm_invoice_line_item  a 
 set
	a.line_item_total_tax=b.line_item_total_tax
from
   temp_brm_invoice_line_item_tax b
where a.invoice=b.invoice
and a.gl_segment=b.gl_segment;


------------------------------------------------------------------------------------------
create or replace temporary table temp_brm_invoice_line_item_audit
as
select
    a.gl_segment,
    a.bill_no,
    bill_end_date,
    IFNULL(line_item_total,0)								   as line_item_total,
    IFNULL(line_item_total_tax,0)							   as line_item_total_tax,
    IFNULL(line_item_total,0)-ifnull(line_item_total_tax,0)		   as line_item_total_no_tax,
    total_due											   as source_current_total,
    cast(total_due-IFNULL(line_item_total,0) as INT64)      as diff_current_total
from
    temp_brm_source_total a 
left outer join
    temp_brm_invoice_line_item b
on a.bill_no=b.invoice
and a.gl_segment=b.gl_segment;

create or replace temporary table temp_brm_cloud_invoice_audit_stage
as
select
    gl_segment,
    bill_no,
    bill_end_date,
    line_item_total,
    line_item_total_tax,
    line_item_total_no_tax,
    source_current_total,
    diff_current_total,
	source_current_total-line_item_total_no_tax		as diff_current_total_no_tax,
    diff_current_total-line_item_total_no_tax		as diff_total_due_no_tax,
     current_datetime()									as tblload_dtt
	 
from
	temp_brm_invoice_line_item_audit
where
    (cast(source_current_total as INT64)<> cast(line_item_total as INT64))
and  bill_no not in(select distinct bill_no from  `rax-staging-dev.stage_one.raw_brm_audit_excludes`);
-------------------------------------------------------------------------------------------
--negative

create or replace temporary table temp_negative_diff
as
select diff_current_total*-1 diff_into_pos,* 
from 
    temp_brm_cloud_invoice_audit_stage 
where   
    diff_current_total <0  
order by
     diff_current_total ;


insert into
    `rax-staging-dev.stage_one.raw_brm_audit_excludes`
select bill_no  from temp_negative_diff  where diff_into_pos < 1;
-------------------------------------------------------------------------------------------     
--positive

create or replace temporary table temp_positve_diff
as
select 
    * 
from 
    temp_brm_cloud_invoice_audit_stage 
where   
    diff_current_total > 0
order by
     diff_current_total;
	 
	 
insert into
    `rax-staging-dev.stage_one.raw_brm_audit_excludes`
select bill_no from temp_positve_diff  
where
    diff_current_total >0
and  diff_current_total <1;

-------------------------------------------------------------------------------------------
Insert into `rax-staging-dev.stage_one.raw_brm_invoice_line_item_audit`(gl_segment,bill_no,bill_end_date,line_item_total,
line_item_total_tax,line_item_total_no_tax,source_current_total,diff_current_total,diff_current_total_no_tax,diff_total_due_no_tax,
tblload_dtt)
select
    gl_segment,
    bill_no,
    CAST(bill_end_date AS DATE) as bill_end_date,
    line_item_total,
    line_item_total_tax,
    line_item_total_no_tax,
    source_current_total,
    diff_current_total,
    source_current_total-line_item_total_no_tax		as diff_current_total_no_tax,
    diff_current_total-line_item_total_no_tax		as diff_total_due_no_tax,
    current_datetime()									as tblload_dtt
from
	temp_brm_cloud_invoice_audit_stage
where
    (source_current_total<> line_item_total)
and  bill_no not in(select distinct bill_no from  `rax-staging-dev.stage_one.raw_brm_audit_excludes`);




END;
