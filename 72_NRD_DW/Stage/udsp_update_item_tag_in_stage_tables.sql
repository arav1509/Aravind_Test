CREATE OR REPLACE PROCEDURE `rax-staging-dev.stage_two_dw.udsp_update_item_tag_in_stage_tables`()
BEGIN


---set xact_abort = on;
-----------------------updating item_tag in stage_dedicated_inv_event_detail table

update `rax-staging-dev`.stage_two_dw.stage_dedicated_inv_event_detail
set item_tag = xit.item_tag
from `rax-staging-dev`.stage_two_dw.stage_dedicated_inv_event_detail sdied
inner join `rax-staging-dev`.stage_two_dw.xref_item_tag xit on sdied.event_type = xit.event_type
and sdied.service_obj_type=xit.service_type 
and sdied.item_type= xit.item_type
where sdied.item_tag is null;

--------------------------updating item_tag in stage_invitemeventdetail table

update `rax-staging-dev`.stage_two_dw.stage_invitemeventdetail 
set item_tag = xit.item_tag
from `rax-staging-dev`.stage_two_dw.stage_invitemeventdetail si
inner join `rax-staging-dev`.stage_two_dw.xref_item_tag xit 
on si.event_type = xit.event_type 
and si.service_obj_type = xit.service_type 
and si.item_type = xit.item_type
where si.item_tag is null;

END;
