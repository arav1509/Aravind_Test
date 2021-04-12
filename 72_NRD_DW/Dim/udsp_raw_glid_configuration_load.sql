CREATE OR REPLACE PROCEDURE `rax-staging-dev.stage_one.udsp_raw_glid_configuration_load`()
BEGIN

create or replace table stage_one.raw_glid_configuration as 
Select 
	Distinct 
		Glid.Rec_Id AS Rec_Id,
		LTRIM(RTRIM(Glid.Descr)) AS Descr,
		LTRIM(RTRIM(ifnull(Glid.Tax_Code,'Unknown'))) AS Tax_Code,
		LTRIM(RTRIM(CFGseg.Name)) AS Name,
		LTRIM(RTRIM(ifnull(Segment.Descr,CFGseg.name))) AS Segment_Descr,
		LTRIM(RTRIM(Segment.Parent_Segment)) AS Parent_Segment,
		LTRIM(RTRIM(ifnull(ParentSegment.Descr,Segment.Parent_Segment))) As Parent_Segement_Description,
		GLAct.Type AS Type,
		GLAct.Attribute AS Attribute,
		LTRIM(RTRIM(GLAct.Gl_Offset_Acct)) AS Gl_Offset_Acct,
		LTRIM(RTRIM(GLAct.GL_Ar_Acct)) AS GL_Ar_Acct,
		Glid.Obj_Id0 AS Obj_Id0,
		current_datetime() AS Record_Created_Datetime
from 
    `rax-landing-qa`.brm_ods.config_t as cfg  
inner join  -- isolate to only /glid config type records
    `rax-landing-qa`.brm_ods.config_glid_t as glid  
on glid.obj_id0 = cfg.poid_id0
inner join  -- get segment level secondary configuration details
    `rax-landing-qa`.brm_ods.config_t as cfgseg   
on lower(cfgseg.poid_type) ='/config/gl_segment'
and lower(cfgseg.value) =concat('0.0.0.1 /config/glid ' , cast(cfg.poid_id0 as string), ' 0')
inner join  -- get account based gl segmentation strings
   `rax-landing-qa`.brm_ods.config_glid_accts_t as glact  
on glid.obj_id0 = glact.obj_id0 and glid.rec_id = glact.rec_id2 and glact.attribute = 1 and glact.type in (5)
inner join `rax-landing-qa`.brm_ods.config_gl_segment_t as segment 
on cfgseg.poid_id0=segment.obj_id0
inner join `rax-landing-qa`.brm_ods.config_gl_segment_t parentsegment 
on segment.parent_segment=parentsegment.segment_name
where 
     ( lower(cfgseg.name) like '%cloud%' or lower(cfgseg.name) like '%dedicated%' or lower(cfgseg.name) like '%email%')
order by glid.obj_id0;

END;
