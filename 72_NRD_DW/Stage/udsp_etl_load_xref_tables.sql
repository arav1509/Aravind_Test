CREATE OR REPLACE PROCEDURE `rax-staging-dev.stage_two_dw.udsp_etl_load_xref_tables`()
BEGIN

    
declare jobdate datetime;
declare	getdate datetime;
declare	tsql string;
declare	min_maxdate datetime;
declare getdate_unix  int64;


create or replace temporary table max_event_date
(
max_event_mod_date datetime
);

insert into max_event_date
select max(event_mod_dtt) from `rax-staging-dev`.stage_two_dw.stage_dedicated_inv_event_detail 
union all
select max(event_mod_date) from `rax-staging-dev`.stage_two_dw.stage_invitemeventdetail
union all
select max(load_date) from `rax-staging-dev`.stage_two_dw.stage_email_apps_inv_event_detail;

set min_maxdate =(select min(max_event_mod_date) from max_event_date);     
set jobdate= datetime_trunc(min_maxdate, day);
set getdate = jobdate  ;    
set getdate_unix = datetime_diff(current_datetime,cast('1970-01-01 00:00:00' as datetime),second);

 

create or replace temporary table temp_xref_item_tag
(
event_type string,
service_type string,
item_type string
);

--------populating incremental data to xref_item_tag table 

insert into temp_xref_item_tag
(select 
e.poid_type as event_type,
e.service_obj_type as service_type,
ebi.item_obj_type as item_type
from `rax-landing-qa.brm_ods`.event_bal_impacts_t ebi
inner join `rax-landing-qa.brm_ods`.event_t e
on ebi.obj_id0=e.poid_id0
where ebi.resource_id<999
and e.rerate_obj_id0 = 0 and ebi.amount <>0
and e.mod_t >= getdate_unix
group by ebi.item_obj_type,e.poid_type ,e.service_obj_type
);

insert into stage_two_dw.xref_item_tag
(
event_type,
service_type,
item_type,
loaded_date,
loaded_by,
modified_date,
modified_by
)
select
txit.event_type,
txit.service_type,
txit.item_type,
current_datetime(),
'etl',
current_datetime(),
'etl'
from temp_xref_item_tag  txit 
left join  `rax-staging-dev.stage_two_dw`.xref_item_tag xit on txit.event_type = xit.event_type and txit.item_type = xit.item_type and txit.service_type = xit.service_type
where xit.event_type is null;


--get all possible item_tag information available in brm objects
create or replace temporary table item_tags
as
select 
it.item_tag,
it.event_type,
it.service_type,
i.item_type 
from `rax-landing-qa.brm_ods`.config_item_tags_t it
inner join `rax-landing-qa.brm_ods`.config_item_types_t i 
on it.item_tag=i.item_tag;
					
--update item_tag based on actual event_type, service_type, item_type				

update `rax-staging-dev`.stage_two_dw.xref_item_tag 
set  item_tag=it.item_tag
from `rax-staging-dev`.stage_two_dw.xref_item_tag i
inner join item_tags it on i.event_type=it.event_type and i.service_type=it.service_type and i.item_type=it.item_type
where 1=1;

--update item_tag based on actual event_type, item_type and service_type as '/account'				

update `rax-staging-dev`.stage_two_dw.xref_item_tag
set item_tag=it.item_tag
from `rax-staging-dev`.stage_two_dw.xref_item_tag i
inner join item_tags it on i.event_type=it.event_type and (case when i.service_type = '' then '/account' else i.service_type end)=it.service_type and i.item_type=it.item_type
where 1=1
;
--update item_tag based on actual event_type, service_type, item_type where service_type as '/account'			
	
update `rax-staging-dev`.stage_two_dw.xref_item_tag
set item_tag=it.item_tag
from `rax-staging-dev`.stage_two_dw.xref_item_tag i
inner join item_tags it on i.event_type like concat(replace(it.event_type,'*',''),'%') and case when i.service_type in('not available','') then '/account' else i.service_type end like concat(replace(it.service_type,'*',''),'%') and i.item_type=it.item_type
where it.event_type<>'/event/*'
;
--update item_tag based on actual item_type and event_type like config_item_tags_t.event_type and service_type as config_item_tags_t.service_type 

update `rax-staging-dev`.stage_two_dw.xref_item_tag
set item_tag=it.item_tag
from `rax-staging-dev`.stage_two_dw.xref_item_tag i
inner join item_tags it
on i.event_type like concat(replace(it.event_type,'*',''),'%') and i.service_type like concat(replace(it.service_type,'*',''),'%') and i.item_type=it.item_type
where it.event_type='/event/*'and i.item_tag is null
;
--update item_tag out of the box item_types

update `rax-staging-dev`.stage_two_dw.xref_item_tag 
set item_tag='payment' 
where lower(item_type)='/item/payment' and item_tag is null;

update `rax-staging-dev`.stage_two_dw.xref_item_tag 
set item_tag='reversal' 
where lower(item_type)='/item/payment/reversal' and item_tag is null;

update `rax-staging-dev`.stage_two_dw.xref_item_tag
set item_tag='adjustment' 
where lower(item_type)='/item/adjustment' and item_tag is null;

update `rax-staging-dev`.stage_two_dw.xref_item_tag
set item_tag='tax' 
where lower(item_type)='/item/cycle_tax' and item_tag is null;

update `rax-staging-dev`.stage_two_dw.xref_item_tag
set item_tag='purchase' 
where lower(item_type)='/item/purchase' and item_tag is null;

update `rax-staging-dev`.stage_two_dw.xref_item_tag
set item_tag='writeoff_reversal'
where lower(event_type)='/event/billing/writeoff_reversal' and lower(item_type)='/item/writeoff_reversal'and item_tag is null;

update `rax-staging-dev`.stage_two_dw.xref_item_tag
set item_tag='write-off'
where lower(event_type)='/event/billing/writeoff/billinfo' and lower(item_type)='/item/writeoff' and item_tag is null;

update `rax-staging-dev`.stage_two_dw.xref_item_tag
set item_tag='cycle_discount'
where lower(event_type)='/event/billing/cycle/discount' and lower(item_type)='/item/misc' and item_tag is null;

update `rax-staging-dev`.stage_two_dw.xref_item_tag
set item_tag='cycle_fold'
where lower(event_type)='/event/billing/cycle/fold' and lower(item_type)='/item/misc' and item_tag is null;

update `rax-staging-dev`.stage_two_dw.xref_item_tag
set item_tag='purchase_fee'
where lower(event_type)='/event/billing/product/fee/purchase' and lower(item_type)='/item/misc' and item_tag is null;

----------------if item_tag value is null we are deriving from item_type values 
update `rax-staging-dev`.stage_two_dw.xref_item_tag
set item_tag = substring(item_type,length(item_type) - strpos(reverse(item_type),'/')+2 ,strpos(reverse(item_type),'/')-1)
where item_tag is null;

drop table temp_xref_item_tag;
drop table max_event_date;

call `rax-staging-dev.stage_two_dw.udsp_xref_item_tag_notifications`();
call `rax-staging-dev.stage_two_dw.udsp_update_item_tag_in_stage_tables`();


 SELECT
    @@error.message,
    @@error.stack_trace,
    @@error.statement_text,
    @@error.formatted_stack_trace;  


END;
