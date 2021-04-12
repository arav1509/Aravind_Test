CREATE OR REPLACE PROCEDURE `rax-abo-72-dev`.report_tables.udsp_etl_dim_support_team_hierarchy()
begin


create or replace temp table teams as
select --into	#teams
	flex_value		as oracle_team_id,
	ffvt.description	as oracle_team,
	created_ts		as creation_date,
	modified_ts		as last_update_date
from
	`rax-landing-qa`.ebs_ods.raw_fnd_flex_values    ffv  
left outer join
	`rax-landing-qa`.ebs_ods.raw_fnd_flex_value_sets   ffvs  
on ffv.flex_value_set_id = ffvs.flex_value_set_id
left outer join
	`rax-landing-qa`.ebs_ods.raw_fnd_flex_values_tl     ffvt   
on ffv.flex_value_id = ffvt.flex_value_id	
inner join
	`rax-landing-qa`.ss_db_ods.team_all ssl 
on ffvt.description = ssl.name
where
	ffvs.flex_value_set_name = 'rs_team'
and (created_ts>=date_add(current_date(), interval -5 day) or  modified_ts >=date_add(current_date(), interval -5 day))
;
-------------------------------------------------------------------------------------------------------------------
create or replace temp table ssl_team_all as 
select --into    #ssl_team_all
 concat(number ,'-',name) as ssl_nk
, team_id
, name
, name as oracle_name
, description
, business_unit
, segment
, region
, subregion
, parent_team_id
, created_ts
, modified_ts
, last_modified_by
, deleted_at
, dw_timestamp
, super_region
, country
, ifnull(subsegment,segment) as subsegment
, number
, number as oracle_id
from
   `rax-landing-qa`.ss_db_ods.team_all ss
where cast(deleted_at as date) ='1970-01-01'
and (created_ts>=date_add(current_date(), interval -5 day) or  modified_ts >=date_add(current_date(), interval -5 day))
;

update ssl_team_all s
set
   s.oracle_name= oracle_team
from
    ssl_team_all a
inner join
    teams b
 on a.number=b.oracle_team_id
where
    a.oracle_name<>a.name;
-------------------------------------------------------------------------------------------------------------------
update ssl_team_all s
set
   s.oracle_id= oracle_team_id
from
    ssl_team_all a
inner join
    teams b
on a.name=b.oracle_team
where
   a.oracle_name<>a.name;
-------------------------------------------------------------------------------------------------------------------
create or replace temp table teams_all  as 
select--into   #teams_all		
     ssl_nk,
	cast(team_id as string) as id,
	country as country,
	super_region as super_region,
	region as region,
	subregion as sub_region,
	segment as team_business_segment,
	segment as team_reporting_segment,
	subsegment as team_business_sub_segment,
	business_unit as team_business_unit,
	business_unit as finance_business_unit,
	oracle_name as team_name,
	oracle_name as team_name_oracle,
	name as team_name_ssl,
	cast(ltrim(rtrim(replace(name,'team ',' ')))as string)	as team_convert,
	subsegment as `grouping`,
	1 as revenue_flag,
	0 as legacy_flag,
	oracle_id as oracle_team_id,
	number as oracle_team_id_ssl,
	super_region as churn_level_1,
	(case 
		when lower(super_region) = 'intl' and lower(subregion) = 'dach' then subregion
		when lower(super_region) ='intl' then lower(region)				
	else 
		segment
	end) as churn_level_2,
	( case 
		when   lower(super_region)='intl' and (lower(region) in ('apac') or     lower(subregion) in ('dach')) then country
		when   lower(super_region)='intl' and (lower(region) not in ('apac') and lower(subregion) not in ('dach')) then business_unit
	else 
		subsegment
	end ) as churn_level_3,
	name	as  churn_grouping,
	1	as  churn_forecast_flag,	
	0	as  churn_is_asp,
	cast('in oracle' as string)					as  ssl_source										
from  ssl_team_all  a
inner join teams b
on a.number=b.oracle_team_id;
-------------------------------------------------------------------------------------------------------------------
insert into teams_all
select
     ssl_nk,
	cast(team_id as string)				as id,
	ifnull(country,'unknown')					as country,
	ifnull(super_region,'unknown')				as super_region,
	ifnull(region, 'unknown')					as region,
	ifnull(subregion,'unknown')					as sub_region,
	ifnull(segment,'unknown')					as team_business_segment,
	ifnull(segment,'unknown')					as team_reporting_segment,
	ifnull(subsegment,'unknown')				as team_business_sub_segment,
	ifnull(business_unit,'unknown')				as team_business_unit,
	ifnull(business_unit,'unknown')				as finance_business_unit,
	ifnull(oracle_name,'unknown')				as team_name,
	ifnull(oracle_name,'unknown')				as team_name_oracle,
	ifnull(name,'unknown')						as team_name_ssl,
	cast(ltrim(rtrim(replace(name,'team ',' ')))as string)	as team_convert,
	ifnull(subsegment,'unknown')				as `grouping`,
	1											as revenue_flag,
	0											as legacy_flag,
	oracle_id										as oracle_team_id,
	number										as oracle_team_id_ssl,
	super_region as churn_level_1,
	(case 
		when super_region = 'intl' and subregion = 'dach' then subregion
		when super_region ='intl' then region
	else 
		segment
	end) as churn_level_2,
	( case 
		when  super_region='intl' and (region in ('apac') or subregion in ('dach')) then country
		when  super_region='intl' and (region not in ('apac') and subregion not in ('dach')) then business_unit
	else 
		subsegment
	end ) as churn_level_3,
	name											as  churn_grouping,
	1											as  churn_forecast_flag,	
	0											as  churn_is_asp,
	'ssl'										as  ssl_source									
from  ssl_team_all  a 
where
   (created_ts>=date_add(current_date(), interval -10 day) or  modified_ts >=date_add(current_date(), interval -10 day))
and oracle_name not in (select team_name from teams_all)
;
-----------------------------------------------------------------------------------   
update teams_all t
set
 t.country='us',
 t.super_region='us',
 t.sub_region='us'
where
	lower(t.region) ='us'
and t.super_region =' ';
-----------------------------------------------------------------------------------  
update teams_all
set
	country='unknown'
where
	(country is null or  country= ' ') ;
----------------------------------------------------------------------------------- 
update teams_all
set
	super_region='unknown'
where
	(super_region is null or  super_region= ' ') ;
----------------------------------------------------------------------------------- 
update teams_all
set
	region='unknown'
where
	(region is null or  region= ' ') ;
----------------------------------------------------------------------------------- 
update teams_all
set
	sub_region='unknown'
where
	(sub_region is null or  sub_region= ' ') ;
----------------------------------------------------------------------------------- 
update teams_all
set
	team_business_segment='unknown'
where
	(team_business_segment is null or  team_business_segment= ' ') ;
----------------------------------------------------------------------------------- 
update teams_all
set
	team_reporting_segment='unknown'
where
	(team_reporting_segment is null or  team_reporting_segment= ' ') ;
----------------------------------------------------------------------------------- 
update teams_all
set
	team_business_sub_segment='unknown'
where
	(team_business_sub_segment is null or  team_business_sub_segment= ' ') ;
----------------------------------------------------------------------------------- 
update teams_all
set
	team_business_unit='unknown'
where
	(team_business_unit is null or  team_business_unit= ' ') ;
----------------------------------------------------------------------------------- 
update teams_all
set
	finance_business_unit='unknown'
where
	(finance_business_unit is null or  finance_business_unit= ' ') ;
----------------------------------------------------------------------------------- 
update `rax-abo-72-dev`.report_tables.dim_support_team_hierarchy a
set
	`grouping`='unknown'
where
	(`grouping` is null or  `grouping`= ' ') ;
-----------------------------------------------------------------------------------
update teams_all
set
	churn_grouping = case when lower(country) like '%au%' and lower(team_business_sub_segment) not in ('aws','smb','cloud','mspc','azure') then 'australia'
						  when lower(country) like '%hk%' and lower(team_business_sub_segment) not in ('aws','smb','cloud','mspc','azure')then 'hong kong'
						  when (lower(country) like '%uk%' and lower(team_business_sub_segment) not in ('aws','smb','cloud','mspc','enterprise','uk marquee colo','uk marquee managed','emea ent 5','emea ent 7','emea ent 8','emea smb 5','emea smb 6','emea smb 7','azure')) 
							   and (lower(country) like '%uk%' and lower(team_business_sub_segment) not like ('team stratus%')) 
							   and (lower(country) like '%uk%' and lower(team_business_sub_segment) not like ('team mc uk%'))then `grouping` -- added uk logic 3/30/17 per biljana j.
						  when lower(country) like '%uk%' and lower(team_name) in ('team int 11','team int 12') then 'emea ent 10' 
						  when lower(country) like '%uk%' and lower(team_name) = 'team uk stratus rglil' then 'emea smb 4'--added 3/30/17 to update special cases uk accounts that do not match above logic
						  when lower(country) like '%us%' and lower(team_name) = 'azure' then 'microsoft azure'
						  else churn_grouping end
              where true
;
-----------------------------------------------------------------------------------
insert into `rax-abo-72-dev`.report_tables.dim_support_team_hierarchy
	(
	id, country, super_region, region, sub_region, team_business_segment, team_reporting_segment, team_business_sub_segment, team_business_unit, finance_business_unit, team_name, team_name_oracle, team_name_ssl, team_convert, `grouping`, revenue_flag, legacy_flag, oracle_team_id, oracle_team_id_ssl, churn_level_1, churn_level_2, churn_level_3, churn_grouping, churn_forecast_flag, churn_is_asp
	)
select distinct
    cast(id as int64) as id,
    country,
    super_region,
    region,
    sub_region,
    team_business_segment,
    team_reporting_segment,
    team_business_sub_segment,
    team_business_unit,
    finance_business_unit,
    team_name,
    team_name_oracle,
    team_name_ssl,
    team_convert,
    `grouping`,
    revenue_flag,
    legacy_flag,
    oracle_team_id,
    oracle_team_id_ssl,
    super_region as churn_level_1,
	(case 
		when lower(super_region) = 'intl' and lower(sub_region) = 'dach' then sub_region
		when lower(super_region) ='intl' then region				
	else 
		team_business_segment
	end) as churn_level_2,
	( case 
		when  lower(super_region)='intl' and (lower(region) in ('apac') or lower(sub_region) in ('dach')) then country
		when  lower(super_region)='intl' and (lower(region) not in ('apac') and lower(sub_region) not in ('dach')) then team_business_unit
	else 
		team_business_sub_segment
	end ) as churn_level_3,
	churn_grouping,
	churn_forecast_flag,
	churn_is_asp						
from teams_all
where 
	 id  not in
		(
		select distinct 
		cast(	id as string)
		from
			`rax-abo-72-dev`.report_tables.dim_support_team_hierarchy
		);
-----------------------------------------------------------------------------------
update `rax-abo-72-dev`.report_tables.dim_support_team_hierarchy d
set
    d.id=cast(b.id as int64),
    d.country=b.country,
    d.super_region=b.super_region,
    d.region=b.region,
    d.sub_region=b.sub_region,
    d.team_business_segment=b.team_business_segment,
    d.team_reporting_segment=b.team_reporting_segment,
    d.team_business_sub_segment=b.team_business_sub_segment,
    d.team_business_unit=b.team_business_unit,
    d.finance_business_unit=b.finance_business_unit,
    d.team_convert=b.team_convert,
    d.`grouping`=b.`grouping`,
    d.oracle_team_id=b.oracle_team_id,
    d.churn_level_1=b.churn_level_1,
    d.churn_level_2=b.churn_level_2,
    d.churn_level_3=b.churn_level_3,
    d.churn_grouping=b.churn_grouping
from `rax-abo-72-dev`.report_tables.dim_support_team_hierarchy a
inner join
   teams_all b
on a.oracle_team_id_ssl=b.oracle_team_id_ssl
where
    lower(b.ssl_source)= 'in oracle'
and  (a.country<>b.country
or cast(a.id as string)<>b.id
or a.super_region<>b.super_region
or a.region<>b.region
or a.sub_region<>b.sub_region
or a.team_business_segment<>b.team_business_segment
or a.team_reporting_segment<>b.team_reporting_segment
or a.team_business_sub_segment<>b.team_business_sub_segment
or a.team_business_unit<>b.team_business_unit
or a.finance_business_unit<>b.finance_business_unit
or a.team_convert<>b.team_convert
or a.`grouping`<>b.`grouping`
or a.oracle_team_id<>b.oracle_team_id
or a.churn_level_1<>b.churn_level_1
or a.churn_level_2<>b.churn_level_2
or a.churn_level_3<>b.churn_level_3
or a.churn_grouping<>b.churn_grouping
);
----------------------------------------------------------------------------------
update `rax-abo-72-dev`.report_tables.dim_support_team_hierarchy d
set
    d.id=cast(b.id as int64),
    d.team_name_ssl=b.team_name_ssl,
    d.churn_grouping=b.churn_grouping
from
    `rax-abo-72-dev`.report_tables.dim_support_team_hierarchy a
inner join
  teams_all b
on a.team_name =b.team_name_ssl
where
    b.ssl_source= 'ssl' 
and( cast(a.id as string)<>b.id
or a.team_name_ssl<>b.team_name_ssl
or a.churn_grouping<>b.churn_grouping
);
--------------------------------------------------------------------
update  `rax-abo-72-dev`.report_tables.dim_support_team_hierarchy d
set
    d.id=cast(b.id as int64),
    d.country=b.country,
    d.super_region=b.super_region,
    d.region=b.region,
    d.sub_region=b.sub_region,
    d.team_business_segment=b.team_business_segment,
    d.team_reporting_segment=b.team_reporting_segment,
    d.team_business_sub_segment=b.team_business_sub_segment,
    d.team_business_unit=b.team_business_unit,
    d.finance_business_unit=b.finance_business_unit,
    d.team_convert=b.team_convert,
    d.`grouping`=b.`grouping`,
    d.oracle_team_id=b.oracle_team_id,
    d.churn_level_1=b.churn_level_1,
    d.churn_level_2=b.churn_level_2,
    d.churn_level_3=b.churn_level_3,
    d.churn_grouping=b.churn_grouping
from `rax-abo-72-dev`.report_tables.dim_support_team_hierarchy a
inner join
   teams_all b
on a.team_name=b.team_name_ssl
where
    b.ssl_source= 'ssl' 
and b.team_business_segment <>''
and b.team_reporting_segment <>''
and(a.country<>b.country
or cast(a.id as string)<>b.id
or a.super_region<>b.super_region
or a.region<>b.region
or a.sub_region<>b.sub_region
or a.team_business_segment<>b.team_business_segment
or a.team_reporting_segment<>b.team_reporting_segment
or a.team_business_sub_segment<>b.team_business_sub_segment
or a.team_business_unit<>b.team_business_unit
or a.finance_business_unit<>b.finance_business_unit
or a.team_convert<>b.team_convert
or a.`grouping`<>b.`grouping`
or a.oracle_team_id<>b.oracle_team_id
or a.churn_level_1<>b.churn_level_1
or a.churn_level_2<>b.churn_level_2
or a.churn_level_3<>b.churn_level_3
or a.churn_grouping<>b.churn_grouping
);
end;