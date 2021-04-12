CREATE OR REPLACE PROCEDURE `rax-abo-72-dev`.report_tables.udsp_etl_revenue_materialization_kickbacks()
begin
/****************************************************************************
**  Last Modified by David Alvarez		
**  8/24/2015			
**  Added to Report_Tables
*****************************************************************************/

create or replace table `rax-abo-72-dev`.report_tables.revenue_materialization_kickbacks as
SELECT DISTINCT
	Opportunity_ID,
	Kickback_Reason_ID,
    Kickback_Reason,
    Kickback_Reason_Group,
    Kickback_Reason_Sales_Group,
    Kickback_Reason_Created,
    Kickback_Reason_Resolved,
    Time_in_Kickback,
    In_Kickback
FROM
	 `rax-abo-72-dev`.report_tables.revenue_materialization A;
end;