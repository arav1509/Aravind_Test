CREATE OR REPLACE PROCEDURE `rax-staging-dev.stage_one.udsp_nrd_raw_gl_account_load`()
BEGIN

create or replace table stage_one.raw_gl_account as
select distinct
      ltrim(rtrim(ifnull(ffv.flex_value,'unknown'))) as flex_value,
	  ltrim(rtrim(ifnull(ffvt.description,'unknown'))) as description,
	  ifnull(ffv.creation_date,'1900-01-01') as creation_date,
	  ffv.end_date_active as end_date_active,
	  current_datetime() as record_created_datetime
from
      `rax-landing-qa`.ebs_ods.raw_fnd_flex_values    ffv  
	  left outer join
       `rax-landing-qa`.ebs_ods.raw_fnd_flex_value_sets   ffvs  
		on ffv.flex_value_set_id = ffvs.flex_value_set_id
	  left outer join
       `rax-landing-qa`.ebs_ods.raw_fnd_flex_values_tl     ffvt   
		on ffv.flex_value_id = ffvt.flex_value_id
where
      lower(ffvs.flex_value_set_name) in ('rs_account');
	  
END;
