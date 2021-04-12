CREATE OR REPLACE PROCEDURE `rax-abo-72-dev`.report_tables.exec_udsp_etl_revenue_materialization_weekly_snapshot()
begin
-------------------------------------------------------------------------------------------------------------
DECLARE DayofWeek string;
DECLARE SuccessfulDate datetime;
SET DayofWeek=(SELECT DayName FROM `rax-abo-72-dev`.report_tables.dimdate WHERE FullDate=(SELECT MAX(CAST(load_date as date)) FROM `rax-abo-72-dev`.report_tables.revenue_materialization));
-------------------------------------------------------------------------------------------------------------
IF lower(DayofWeek)='wednesday' then
     call `rax-abo-72-dev`.report_tables.udsp_etl_revenue_materialization_weekly_snapshot();
	
end if;
END;