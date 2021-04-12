CREATE OR REPLACE PROCEDURE `rax-abo-72-dev`.sales.exec_udsp_etl_partner_compensation()
BEGIN
DECLARE	return_value int64;
DECLARE CurrentMonth date;
DECLARE PriorMonth date;
DECLARE PriorMonthWorkDays int64;
DECLARE CurrentMonthWorkDays int64;
DECLARE CalDays int64;
-------------------------------------------------------------------------
SET CurrentMonth=CURRENT_DATE();
SET PriorMonth=`rax-abo-72-dev`.bq_functions.udf_lastdayofpreviousmonth(CurrentMonth);
SET PriorMonthWorkDays =  (SELECT PriorMonthWorkDays   FROM `rax-abo-72-dev`.sales.reporting_monthly_load_busines_day_start_stop    WHERE UPPER(Reporting_Group_Type) = 'BOOKING METRICS' AND Reporting_Month_Key=`rax-abo-72-dev`.bq_functions.udf_yearmonth_nohyphen(CurrentMonth)); 
SET CurrentMonthWorkDays =(SELECT CurrentMonthWorkDays FROM `rax-abo-72-dev`.sales.reporting_monthly_load_busines_day_start_stop WHERE UPPER(Reporting_Group_Type) = 'BOOKING METRICS' AND Reporting_Month_Key=`rax-abo-72-dev`.bq_functions.udf_yearmonth_nohyphen(CurrentMonth));
-------------------------------------------------------------------------------------------------------------
set CalDays =
	(select
		COUNT(FullDate)
	 from
		`rax-abo-72-dev`.sales.dimdate
	 Where
		FullDate >= `rax-abo-72-dev`.bq_functions.udf_firstdayofmonth(CurrentMonth) 
	and FullDate <= CurrentMonth
	AND Isworkday='Y'
	 );
-------------------------------------------------------------------------------------------------------------
IF  
	CalDays <= PriorMonthWorkDays  --callUTE ENT ACQ for prior month unit 5th business day of current month
then
	call `rax-abo-72-dev`.sales.udsp_etl_partner_accounts_all(V_Date=PriorMonth);
		select  'prior month udsp_etl_partner_accounts_all complete';
	call `rax-abo-72-dev`.sales.udsp_etl_partner_program_accounts_all(V_Date=PriorMonth);
		select  'prior month udsp_etl_partner_program_accounts_all complete';
	call `rax-abo-72-dev`.sales.udsp_etl_partner_program_line_item_detail(V_Date=PriorMonth);
	call `rax-abo-72-dev`.sales.udsp_etl_dim_partner_nontraditional_products();
		select  'prior month udsp_etl_partner_program_line_item_detail complete';
	call `rax-abo-72-dev`.sales.udsp_etl_partner_compensation_referral(V_Date=PriorMonth);
		select  'prior month udsp_etl_partner_compensation_referral complete';
	call `rax-abo-72-dev`.sales.udsp_etl_partner_compensation_reseller(V_Date=PriorMonth);
		select  'prior month udsp_etl_partner_compensation_reseller complete';
	call `rax-abo-72-dev`.sales.udsp_etl_partner_compensation_strategic(V_Date=PriorMonth);
		select  'prior month udsp_etl_partner_compensation_strategic complete';
	call `rax-abo-72-dev`.sales.udsp_etl_partner_compensation_vc_promo(V_Date=PriorMonth);
		select  'prior month udsp_etl_partner_compensation_vc_promo complete';
	call `rax-abo-72-dev`.sales.udsp_etl_partner_all();
	call `rax-abo-72-dev`.sales.udsp_etl_partner_all_v2(V_Date=PriorMonth);
	call `rax-abo-72-dev`.sales.udsp_etl_partner_all_line_item_detail();
	call `rax-abo-72-dev`.sales.udsp_etl_partner_all_line_item_detail_v2();
elseif 
    CalDays >= CurrentMonthWorkDays --callUTE ENT ACQ for current month after 7 business day  of current month
then
	call `rax-abo-72-dev`.sales.udsp_etl_Partner_Accounts_All(V_Date=CurrentMonth);
		select  'curr month udsp_etl_Partner_Accounts_All complete';
	call `rax-abo-72-dev`.sales.udsp_etl_Partner_Program_Accounts_All(V_Date=CurrentMonth);
		select  'curr month udsp_etl_Partner_Program_Accounts_All complete';
	call `rax-abo-72-dev`.sales.udsp_etl_Partner_Program_Line_Item_Detail(V_Date=CurrentMonth);
	call `rax-abo-72-dev`.sales.udsp_etl_Dim_Partner_NonTraditional_Products(V_Date=CurrentMonth);
		select  'curr month udsp_etl_Partner_Program_Line_Item_Detail complete';
	call `rax-abo-72-dev`.sales.udsp_etl_Partner_Compensation_Referral(V_Date=CurrentMonth);
		select  'curr month udsp_etl_Partner_Compensation_Referral complete';
	call `rax-abo-72-dev`.sales.udsp_etl_Partner_Compensation_Reseller(V_Date=CurrentMonth);
		select  'curr month udsp_etl_Partner_Compensation_Reseller complete';
	call `rax-abo-72-dev`.sales.udsp_etl_Partner_Compensation_Strategic(V_Date=CurrentMonth);
		select  'curr month udsp_etl_Partner_Compensation_Strategic complete';
	call udsp_etl_Partner_Compensation_VC_Promo(V_Date=CurrentMonth);
		select  'curr month udsp_etl_Partner_Compensation_VC_Promo complete';
	call `rax-abo-72-dev`.sales.udsp_etl_Partner_All();
	call `rax-abo-72-dev`.sales.udsp_etl_Partner_All_v2(V_Date=CurrentMonth);
	call `rax-abo-72-dev`.sales.udsp_etl_Partner_All_Line_Item_Detail();
	call `rax-abo-72-dev`.sales.udsp_etl_Partner_All_Line_Item_Detail_v2();

end if;

end;
