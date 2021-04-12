CREATE OR REPLACE PROCEDURE `rax-abo-72-dev`.sales.udsp_populate_partner_compensation_tables()
begin
---------------------------------------------------------------------------------------
/*
Created On: 3/23/2016
Created By: David Alvarez

Description:
Loads Partner Compensation Tables


Modifications:
--------------

Modified By          Date     Description
-----------------  ---------- ----------------------------------------------------------

----------------------------------------------------------------------------------------
*/
----------------------------------------------------------------------------------------
call `rax-abo-72-dev`.sales.exec_udsp_etl_partner_compensation()
end;
