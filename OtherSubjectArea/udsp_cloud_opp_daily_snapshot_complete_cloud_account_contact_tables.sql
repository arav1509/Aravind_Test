CREATE OR REPLACE PROCEDURE `rax-abo-72-dev`.sales.udsp_cloud_opp_daily_snapshot_complete_cloud_account_contact_tables()
begin
/*
KVC

It will either confirm that the SA3 tables are ready for extraction, or that the job 
needs to try again in 5 minutes before continuing.

Modification History:

*/
Declare v_Date datetime;
Declare v_Getdate datetime;
Set v_Getdate = ifnull((
				SELECT 
					MIN(Load_Date)		AS MIN_Date
				FROM
					`rax-abo-72-dev`.sales.opp_daily_snapshot_flag A
				WHERE
					Load_Flag=1
				), '1900-01-01');
				
Set v_Date = (SELECT `rax-abo-72-dev`.bq_functions.udfdatepart(current_datetime()));
/*
PRINT month(@Date)
PRINT day(@date) 
PRINT year(@date)

Print v_Date
Print v_Getdate
*/
if v_Getdate = v_Date then
    select 'Success';
else
	RAISE USING MESSAGE = '<h3 style="background-color:DodgerBlue;">EXCEPTION: Check for load of Cloud_Opportunity_Daily_SnapShot needed to Load Step 2 Load Cloud Account Contact Tables In job Load Cloud Account Contact Tables</h3>';
end if;

end;
