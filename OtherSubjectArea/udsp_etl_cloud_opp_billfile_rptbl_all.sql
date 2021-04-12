CREATE OR REPLACE PROCEDURE `rax-abo-72-dev`.sales.udsp_etl_cloud_opp_billfile_rptbl_all()
begin
---------------------------------------------------------------------------------------
/*
Created On: 1/20/2012
Created By: Kano Cannick

Description:
Runs all the procs needed to populate report tables.


Modifications:
--------------

Modified By          Date     Description
-----------------  ---------- ----------------------------------------------------------

----------------------------------------------------------------------------------------
*/
----------------------------------------------------------------------------------------

DECLARE bgn_Key INT64;
DECLARE end_Key INT64;
----------------------------------------------------------------------------------------
SET bgn_Key = `rax-abo-72-dev`.bq_functions.udf_time_key_nohyphen(current_date()-7);
----------------------------------------------------------------------------------------

call `rax-abo-72-dev`.sales.exec_udsp_etl_cloud_smb_acq_email_apps_all();
call `rax-abo-72-dev`.sales.exec_udsp_etl_cloud_smb_ib_email_apps_all();
end;
