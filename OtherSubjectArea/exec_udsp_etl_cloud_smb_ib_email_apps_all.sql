CREATE OR REPLACE PROCEDURE `rax-abo-72-dev`.sales.exec_udsp_etl_cloud_smb_ib_email_apps_all()
begin

DECLARE	return_value int64;
DECLARE CurrentMonth date;
DECLARE PriorMonth date;
DECLARE PriorMonthWorkDays int64;
DECLARE CurrentMonthWorkDays int64;
DECLARE CalDays int64;
-------------------------------------------------------------------------
SET CurrentMonth=current_date();
SET PriorMonth=`rax-abo-72-dev`.bq_functions.udf_lastdayofpreviousmonth(CurrentMonth);
SET PriorMonthWorkDays = (SELECT PriorMonthWorkDays 
						FROM `rax-abo-72-dev`.sales.reporting_monthly_load_busines_day_start_stop 
						WHERE Reporting_Group_Type= 'Bill File' AND Reporting_Month_Key=`rax-abo-72-dev`.bq_functions.udf_yearmonth_nohyphen(CurrentMonth)
						); 
SET CurrentMonthWorkDays =(SELECT CurrentMonthWorkDays 
							FROM `rax-abo-72-dev`.sales.reporting_monthly_load_busines_day_start_stop
							WHERE Reporting_Group_Type= 'Bill File' 
							AND Reporting_Month_Key=`rax-abo-72-dev`.bq_functions.udf_yearmonth_nohyphen(CurrentMonth)
							);
-------------------------------------------------------------------------------------------------------------
set CalDays =
	(select
		COUNT(FullDate)
	 from
		`rax-abo-72-dev`.sales.dimdate
	 Where
		FullDate >= `rax-abo-72-dev`.bq_functions.udf_firstdayofmonth(current_date()) 
	and FullDate <= current_date()
	AND Isworkday='Y'
	 );
-------------------------------------------------------------------------------------------------------------
IF  
	CalDays <= PriorMonthWorkDays  then--EXECUTE ENT ACQ for prior month unit 3rd business day of current month
	call `rax-abo-72-dev`.sales.udsp_etl_cloud_smb_ib_email_apps_all(PriorMonth);
ELSEIF CalDays >= CurrentMonthWorkDays then --EXECUTE ENT ACQ for current month after 7 business day  of current month
	call `rax-abo-72-dev`.sales.udsp_etl_cloud_smb_ib_email_apps_all(CurrentMonth);
end if;
end;
