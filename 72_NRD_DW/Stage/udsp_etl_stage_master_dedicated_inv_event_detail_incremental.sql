CREATE OR REPLACE PROCEDURE `rax-staging-dev.stage_one.udsp_etl_stage_master_dedicated_inv_event_detail_incremental`()
BEGIN

call `rax-staging-dev`.stage_two_dw.udsp_etl_stage_dedicated_gl_codes_brm();
call `rax-staging-dev`.stage_two_dw.udsp_etl_stage_dedicated_gl_products_brm();
call `rax-staging-dev`.stage_two_dw.udsp_etl_stage_dedicated_inv_event_detail_incremental();


END;
