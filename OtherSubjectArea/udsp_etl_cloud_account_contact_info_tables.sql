
CREATE OR REPLACE PROCEDURE `rax-abo-72-dev`.sales.udsp_etl_cloud_account_contact_info_tables()
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

call `rax-abo-72-dev`.sales.udsp_load_cloud_us_contact_info();

call `rax-abo-72-dev`.sales.udsp_load_cloud_uk_contact_info();

call `rax-abo-72-dev`.sales.udsp_load_cloud_email_apps_contact_info();

end;
