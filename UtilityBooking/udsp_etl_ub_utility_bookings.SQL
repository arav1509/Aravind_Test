CREATE OR REPLACE PROCedure `rax-abo-72-dev`.sales.udsp_etl_ub_utility_bookings()
begin
---------------------------------------------------------------------------------------
/*
Created On: 1/31/2018
Created By: D Alvarez

Description:
Runs all the procs needed to populate Utility Bookings Attribute Tables


Modifications:
--------------

Modified By          Date     Description
-----------------  ---------- ----------------------------------------------------------


*/



--udsp_etl_UB_Account_Ownership

---------------------------------------------------------------------------------------
CALL `rax-abo-72-dev`.sales.udsp_etl_ub_account_ownership();


--udsp_etl_UB_Upfront_Bookings

CALL `rax-abo-72-dev`.sales.udsp_etl_ub_upfront_bookings();

---------------------------------------------------------------------------------------

--udsp_etl_UB_Bridge_Owner_Hierarchy

CALL `rax-abo-72-dev`.sales.udsp_etl_ub_bridge_owner_hierarchy();
---------------------------------------------------------------------------------------



--udsp_etl_UB_RT_Utility_Bookings

CALL `rax-abo-72-dev`.sales.udsp_etl_ub_rt_utility_bookings();



END;
