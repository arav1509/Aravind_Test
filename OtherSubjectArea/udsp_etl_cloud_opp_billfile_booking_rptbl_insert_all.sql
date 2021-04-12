create or replace procedure `rax-abo-72-dev`.sales.udsp_etl_cloud_opp_billfile_booking_rptbl_insert_all()
begin
---------------------------------------------------------------------------------------
/*
created on: 1/20/2012
created by: kano cannick
description:
runs all the procs needed to populate report tables.
modifications:
--------------
modified by          date     description
-----------------  ---------- ----------------------------------------------------------
select * from report_tables_duration_log where group in('cloud bill-file booking insert all','cloud smb activations','cloud smb invoicing','cloud smb invoice audit','cloud ent activations') and time_key=20120926 order by start_time desc
----------------------------------------------------------------------------------------
*/
call `rax-abo-72-dev`.sales.exec_udsp_etl_cloud_smb_ib_acq_email_apps_invoicing_insert();--ok
call `rax-abo-72-dev`.sales.exec_udsp_etl_email_apps_service_line_item();--ok
call `rax-abo-72-dev`.sales.exec_udsp_etl_cloud_smb_ib_acq_email_apps_invoicing_detail();
call `rax-abo-72-dev`.sales.exec_udsp_etl_cloud_smb_ib_acq_email_apps_invoicing_specialist(); --opportunity  column not find
call `rax-abo-72-dev`.sales.exec_udsp_etl_cloud_smb_ib_acq_email_apps_invoicing_partner_role();
call `rax-abo-72-dev`.sales.exec_udsp_etl_cloud_smb_ib_acq_email_apps_invoice_audit();

end;