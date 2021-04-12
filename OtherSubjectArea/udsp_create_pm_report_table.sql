CREATE OR REPLACE PROCEDURE `rax-abo-72-dev`.report_tables.udsp_create_pm_report_table()
BEGIN
/*This Part Generates the Report*/



Declare RunTime STRING;
Declare StartTime datetime default current_date();
Declare RunPeriod datetime;

Set RunPeriod = date_add(starttime, interval -18 month);


--00:01
create or replace temp table temp11 as
select --into #temp11
      Typex FINAL_OPPORTUNITY_TYPE,booking, 
         case when lower(owner_role) like 'us %' then 'americas' else 'intl' end as opp_owner_region,
      currencyisocode,lower(owner_role) owner_role,ownerid,category,
      --split_category,
      opportunity_id as opp_number,stagename,closedate,
      approval_amount,cast(max_lead_role as string) as max_lead_role,extract(month from Max_Date_Passed) LEAD_PASSED_MONTH, max_date_passed,
      accountid,leadsource,typex, id, amount,0 CONFIRMED_AMOUNT,quote_id, createddate, namex as opp_name,Typex OPPORTUNITY_TYPE,territory, ddi, support_segment
from 
     `rax-landing-dev`.salesforce_ods.qopportunity
where
		 delete_flag = 'n'
    and on_demand_reconciled='false'
       --and createddate >= runperiod --and '11/30/2013'
--     and opportunity_id = '1285755'
       --and stagename like '%won%'
       and category is not null
       --and opportunity_type not like '%mail%' and opportunity_type not like '%cloud%'
       and ( lower(Typex) not in ('mail','cloud','revenue ticket','professional services','managed cloud') or Typex is null)
       and lower(owner_role) not like '%mail%'
       and lower(owner_role) not like '%cloud%'
       and lower(owner_role) not like '%mosso%'
       and lower(owner_role) not like '%slicehost%'
       and (lower(owner_role) like 'us %' or lower(owner_role) like 'apac %' or lower(owner_role)  like 'australia %' or lower(owner_role) like 'benelux %' or lower(owner_role) like 'emea %' or lower(owner_role) like 'dach %' or lower(owner_role) like 'hong kong %')
;


--00:01  
create or replace temp table temp2a1 as    
Select distinct 
      FINAL_OPPORTUNITY_TYPE,BOOKING,
      a.CURRENCYISOCODE,b.OWNER_ROLE,CATEGORY,Opp_Owner_Region,
      null SPLIT_CATEGORY,
      opp_number,STAGENAME,CLOSEDATE,
      APPROVAL_AMOUNT,MAX_LEAD_ROLE,LEAD_PASSED_MONTH,MAX_DATE_PASSED,
      leadsource,a.typex ,0 CORE_ACCOUNT_NUMBER,b.NAMEX as account_name, a.OwnerID,
         a.amount,quote_id, a.createddate, b.typex as customer_type, a.DDI, b.support_segment,
         opp_name, a.territory, OPPORTUNITY_TYPE, b.id as accountid, a.id as oppid
--into #temp2a1
from temp11 a
inner join `rax-landing-dev`.salesforce_ods.qaccount b 
      on ACCOUNTID = b.id
where b.delete_flag = 'N'
       and lower(b.typex) in ('prospect','customer','former customer')
;

create or replace temp table temp21 as 
select distinct a.*, u.namex as Opportunity_Owner,U.title as sales_title
--into #temp21
from temp2a1 a 
inner join `rax-landing-dev`.salesforce_ods.quser U  
on a.ownerid = u.id
where U.delete_flag = 'N'
;


create or replace temp table temp31 as
select *
,case when stagename like '%Won%' then 'Closed Won' else 'not closed won' end as Win_State 
--into #temp31
--SELECT DISTINCT ownER_ROLE
from temp21 
where category is not null
--and opportunity_type not like '%mail%' and opportunity_type not like '%cloud%'
and ( lower(typex) not in ('mail','cloud','revenue ticket','professional services','managed cloud') or typex is null)
and lower(customer_type) in ('prospect','customer','former customer')
and lower(owner_role) not like '%mail%'
and lower(owner_role) not like '%cloud%'
and lower(owner_role) not like '%mosso%'
and lower(owner_role) not like '%slicehost%';

create or replace temp table tempPMQuotes as
select * --into #tempPMQuotes 
from `rax-landing-dev`.proposal_manager_ods.quotes   
where pricing_eligible is true
;


create or replace temp table tempPMopp as
select ID, OPPORTUNITY_NUMBER, SALES_REP_SSO  
--into #tempPMopp  
from `rax-landing-dev`.proposal_manager_ods.opportunities  b  
       WHERE Created_AT >= runPeriod;

create or replace temp table temp41 as
select 
       a.*, b.opportunity_number
--into #temp41
from  tempPMQuotes  A
INNER JOIN 
	tempPMopp B
on 
       a.opportunity_id = b.id
where cast(opportunity_number as string) in (select opp_number from temp31)
;


create or replace temp table PM_base1 as
select * 
--into #PM_base1
from (
       select *, 0 as sf_ind from
             (select * from temp31 where quote_id is not null) a
       inner join 
             temp41 B
       on cast(replace(quote_id,',','') as numeric) =b.id

       union all

       select *, 1 as sf_ind from
             (select * from temp31 where quote_id is null) Y
       inner join
             (select a.*
             from temp41 A
             inner join 
       (select a.opportunity_number, max(id) as id
             from temp41 A
             inner join 
                    (select opportunity_number, max(updated_at) as max_updated_at  from temp41 group by opportunity_number) B
             on a.opportunity_number = b.opportunity_number
             and a.updated_at = b.max_updated_at
             group by a.opportunity_number)X
             on a.opportunity_number = x.opportunity_number
             and a.id = x.id) X
       on Y.opp_number = cast(X.opportunity_number as string)
       )Z
;



create or replace temp table part11 as
select *
--into #part11
       from `rax-landing-dev`.proposal_manager_ods.options
       where is_primary is true and pricing_applies is true 
       and quote_id in (select id from PM_base1)
;
create or replace temp table part41 as
select * --into #part41 
from `rax-landing-dev`.proposal_manager_ods.configurations where created_At >= runPeriod
and  option_id in (select id from part11) 
;
create or replace temp table part51 as
select * --into #part51 
from `rax-landing-dev`.proposal_manager_ods.configuration_items where created_At >= runPeriod
and  configuration_id in (select id from part41);

create or replace temp table part21 as
select   e.item_id--into #part21
		, sum(list_cents) as list_cents
       , sum(entered_cents) as entered_cents
       , sum(min_cents) min_cents
       , sum(max_cents) as max_cents
       ,  sum(default_entered_cents) as default_entered_cents
       ,sum(adjusted_list_cents) as adjusted_list_cents
       --,case when h.id in (1,3,5,43,45,49,54,55,58,59,60,75) then 'rep discountable' else 'no rep discount' end as discount_ind    --this may be outdated hardcoding
       ,max( e.quantity) as quantity
from   `rax-landing-dev`.proposal_manager_ods.prices e  
where   item_type = 'ConfigurationItem' and price_type_id in (4,5,7,9)
	and e.item_id in (select id from part51)
       group by item_id
       order by item_id
;

create or replace temp table part31 as
select id, name --into #part31
from `rax-landing-dev`.proposal_manager_ods.segments 
;

create or replace temp table part61 as
select * 
--into  #part61 
from `rax-landing-dev`.proposal_manager_ods.line_items where created_At >= runPeriod
;

--drop table #PM_base2
create or replace temp table pm_base21 as
select a.opp_number, a.createddate, a.closedate,customer_type,ifnull(a.days_free,0) as days_free,
a.stagename, a.category, i.name as segment, a.territory, DDI
,a.contract_term_length, c.option_id, b.quote_id, active_core_devices, migration_period
, d.id as line_item_id, Win_State, owner_role, final_opportunity_type
,case when price_matched_device_number is not null then 1 else 0 end as PMatch_ind
,break_even_discount
,silver_bullet_discount
, cast(e.list_cents/100 as numeric) as unit_list
, cast(e.entered_cents/100 as numeric) as unit_final_price
, cast(e.min_cents/100 as numeric) as unit_min_price
, cast(e.max_cents/100 as numeric) as unit_max_price
, cast(e.default_entered_cents/100 as numeric) as unit_default_entered_cents
, cast(e.adjusted_list_cents/100 as numeric) as unit_adjusted_list_cents
--,case when h.id in (1,3,5,43,45,49,54,55,58,59,60,75) then 'rep discountable' else 'no rep discount' end as discount_ind    --this may be outdated hardcoding
, e.quantity
, g.name
, h.name as typex
--into #pm_base21
from  PM_base1 A
inner join part11 B
on a.id = b.quote_id
inner join  part41 C 
on b.id = c.option_id
inner join  part51 D 
on c.id = d.configuration_id
inner join part21 E
on d.id = e.item_id
inner join part31 i
on a.segment_id = i.id
inner join part61 F 
on d.line_item_id = f.id
inner join 
`rax-landing-dev`.proposal_manager_ods.line_item_templates g 
on f.line_item_template_id = g.id
inner join 
`rax-landing-dev`.proposal_manager_ods.line_item_categories h 
on f.line_item_category_id = h.id
;

--drop table #pm_base3
create or replace temp table pm_base31 as
select--into #pm_base31
       opp_number
, createddate
,closedate
,customer_type,
stagename
,category
,segment
, territory
,days_free
, migration_period
, DDI
, min(option_id) as option_id
, quote_id
, active_core_devices
,ifnull(max(contract_term_length),0) as contract_term_length
,max(Win_State) as Win_State
, max(owner_role) as owner_role
, max(final_opportunity_type) as final_opportunity_type
,max(cast(break_even_discount as int64)) as break_even_discount
,max(cast(silver_bullet_discount as int64 )) as silver_bullet_discount
,sum(total_list) as total_list
,sum(total_final) as total_final
,sum(total_min) as total_min
,sum(total_max) as total_max
,sum(total_default_entered) as total_default_entered
,sum(total_adjusted_list) as total_adjusted_list
,max(PMatch_ind) as PMatch_ind
from
(select *
, unit_list*quantity as total_list
, unit_final_price*quantity as total_final
,unit_min_price*quantity as total_min
,unit_max_price*quantity as total_max
,unit_default_entered_cents*quantity as total_default_entered
,unit_adjusted_list_cents*quantity as total_adjusted_list
from pm_base21)X
group by     opp_number, createddate,closedate,customer_type,
stagename,category,segment, territory,days_free, migration_period, DDI, quote_id, active_core_devices
;

create or replace temp table pm_baseX1 as
select --into #pm_baseX1
a.quote_id, a.opp_number, a.createddate, a.closedate,customer_type, stagename
,days_free, migration_period
,Win_State, owner_role, final_opportunity_type , category, segment, territory, contract_term_length
, total_list, total_final, total_min, total_max
,total_default_entered
, total_adjusted_list
,PMatch_ind, active_core_devices
,a.break_even_discount
,a.silver_bullet_discount
,max(term_discount) as term_discount, max(maximum_category_discount) as cat_disc, max(bulk_discount) as bulk_disc, min(days_free_discount) as days_free_disc, max(b.silver_bullet_discount) as silver_bullet, max(b.break_even_discount) as break_even_disc
from  pm_base31 a
left join
       `rax-landing-dev`.proposal_manager_ods.option_discount_categories b 
on a.option_id = b.option_id     
group by a.quote_id, a.opp_number, a.createddate, a.closedate,customer_type
,days_free, migration_period, stagename
,Win_State, owner_role, final_opportunity_type , category, segment, territory, contract_term_length, total_list, total_final, total_min, total_max
,total_default_entered
, total_adjusted_list
,Pmatch_ind, active_core_devices
,a.break_even_discount
,a.silver_bullet_discount
;
create or replace temp table approval_base as
select  * --into #approval_base
from  `rax-landing-dev`.proposal_manager_ods.approvals 
where approval_type_id in (16, 17)
and Created_AT >= runPeriod
and lower(status) = 'approved'
;

create or replace temp table tempQ1 as
--Get Deep Dive Info
select --into #tempQ1
distinct a.*
,case when c.opportunity_number  is null then 'no strategic pricing given' else 'strategic pricing given' end as strategic_price_ind
,case when f.opportunity_number  is null then 'No Custom One Off' else 'Custom One Off' end as one_off_ind
,extract(month from a.createddate) as time_month, extract(year from a.createddate) as time_year
from pm_baseX1 a
left join
       (select distinct opportunity_number 
from 
(select * 
       from  approval_base 
       where approval_type_id = 17
       and lower(status) = 'approved'
       ) a
left join
       (select * from temp41) B
on a.quote_id = b.id
) c
on a.opp_number = cast(c.opportunity_number  as string)
left join
       (select distinct opportunity_number 
from 
(
       select * 
       from  approval_base 
       where approval_type_id = 16
       and lower(status) = 'approved'
) a
left join
       (select * from temp41) B
on a.quote_id = b.id
) f
on a.opp_number = cast(f.opportunity_number  as string)
;


----Raw Data on PM deals
create or replace table `rax-abo-72-dev`.report_tables.pm_reporting_table_uk as
select 
a.quote_id,
a.opp_number,
a.createddate,
a.closedate,
a.customer_type,
a.stagename,
a.days_free,
a.migration_period,
a.Win_State,
a.owner_role,
a.final_opportunity_type,
a.category,
a.segment,
a.territory,
a.contract_term_length,
a.total_list,
a.total_final,
a.total_min,
a.total_max,
--a.total_default_entered,
a.total_adjusted_list,
case when a.total_list = 0 then 0 else 1.0-a.total_adjusted_list/a.total_list end as auto_discounting,
case when a.total_adjusted_list = 0 then 0 else 1.0-a.total_final/a.total_adjusted_list end as manual_discounting,
case when a.total_list = 0 then 0 else  1.0-a.total_final/a.total_list end as total_discounting,
a.PMatch_ind,
--a.active_core_devices,
a.break_even_discount,
a.silver_bullet_discount,
a.term_discount,
case when cast(a.cat_disc as int64) <> 0 then 1 else 0 end as categry_discount,
case when cast(a.bulk_disc  as int64) <> 0 then 1 else 0 end as bulk_discount,
case when cast(a.days_free_disc as int64) <>  0 then 1 else 0 end as days_free_Discount,
a.strategic_price_ind,
a.one_off_ind,
a.time_month,
a.time_year,
b.BOOKING,
b.CURRENCYISOCODE,
--b.OWNER_ROLE,
--b.CATEGORY,
b.SPLIT_CATEGORY,
--b.STAGENAME,
--b.CLOSEDATE,
b.APPROVAL_AMOUNT,
b.MAX_LEAD_ROLE,
b.LEAD_PASSED_MONTH,
b.MAX_DATE_PASSED,
b.leadsource,
b.typex,
b.CORE_ACCOUNT_NUMBER,
b.account_name,
b.OwnerID,
b.amount,
--b.quote_id,
--b.createddate,
--b.customer_type,
b.DDI,
b.support_segment,
b.opp_name,
--b.territory,
b.OPPORTUNITY_TYPE,
b.accountid,
b.oppid,
b.Opportunity_Owner,
b.sales_title
--b.Win_State
--into dbo.PM_reporting_table_UK
from tempQ1 a
left join temp31 b
on a.opp_number = b.opp_number
where lower(Opp_Owner_Region) = 'intl'
;


create or replace table `rax-abo-72-dev`.report_tables.pm_reporting_table as
----Raw Data on PM deals
select 
a.quote_id,
a.opp_number,
a.createddate,
a.closedate,
a.customer_type,
a.stagename,
a.days_free,
a.migration_period,
a.Win_State,
a.owner_role,
a.final_opportunity_type,
a.category,
a.segment,
a.territory,
a.contract_term_length,
a.total_list,
a.total_final,
a.total_min,
a.total_max,
--a.total_default_entered,
a.total_adjusted_list,
case when a.total_list = 0 then 0 else 1.0-a.total_adjusted_list/a.total_list end as auto_discounting,
case when a.total_adjusted_list = 0 then 0 else 1.0-a.total_final/a.total_adjusted_list end as manual_discounting,
case when a.total_list = 0 then 0 else  1.0-a.total_final/a.total_list end as total_discounting,
a.PMatch_ind,
--a.active_core_devices,
a.break_even_discount,
a.silver_bullet_discount,
a.term_discount,
case when cast(a.cat_disc as int64) <> 0 then 1 else 0 end as categry_discount,
case when cast(a.bulk_disc  as int64)<> 0 then 1 else 0 end as bulk_discount,
case when cast(a.days_free_disc  as int64) <>  0 then 1 else 0 end as days_free_Discount,
a.strategic_price_ind,
a.one_off_ind,
a.time_month,
a.time_year,
b.BOOKING,
b.CURRENCYISOCODE,
--b.OWNER_ROLE,
--b.CATEGORY,
b.SPLIT_CATEGORY,
--b.STAGENAME,
--b.CLOSEDATE,
b.APPROVAL_AMOUNT,
b.MAX_LEAD_ROLE,
b.LEAD_PASSED_MONTH,
b.MAX_DATE_PASSED,
b.leadsource,
b.typex,
b.CORE_ACCOUNT_NUMBER,
b.account_name,
b.OwnerID,
b.amount,
--b.quote_id,
--b.createddate,
--b.customer_type,
b.DDI,
b.support_segment,
b.opp_name,
--b.territory,
b.OPPORTUNITY_TYPE,
b.accountid,
b.oppid,
b.Opportunity_Owner,
b.sales_title
--b.Win_State
--into dbo.PM_reporting_table
from tempQ1 a
left join temp31 b
on a.opp_number = b.opp_number
where lower(Opp_Owner_Region) = 'intl'
;

end;