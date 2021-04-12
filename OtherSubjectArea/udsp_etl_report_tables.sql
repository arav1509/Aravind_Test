CREATE OR REPLACE PROCEDURE `rax-abo-72-dev`.report_tables.udsp_etl_report_tables()
BEGIN

call `rax-abo-72-dev`.report_tables.udsp_etl_dim_support_team_hierarchy();--ok

call `rax-abo-72-dev`.report_tables.udsp_send_alert_new_support_team_added();--ok
 
call `rax-abo-72-dev`.report_tables.udsp_new_churn_grouping_alert_added();--ok

call `rax-abo-72-dev`.report_tables.udsp_weeklyppt_growth();--- column missing Split_Category not found inside --ok

call `rax-abo-72-dev`.report_tables.udsp_weeklyppt_pipelines();--ok

call `rax-abo-72-dev`.report_tables.udsp_etl_dim_sku_device();--ok

call `rax-abo-72-dev`.report_tables.udsp_etl_revenue_materialization();--ok

call `rax-abo-72-dev`.report_tables.exec_udsp_etl_revenue_materialization_weekly_snapshot();--ok

call `rax-abo-72-dev`.report_tables.udsp_etl_revenue_materialization_om();--ok

call `rax-abo-72-dev`.report_tables.udsp_etl_revenue_materialization_kickbacks();--ok

call `rax-abo-72-dev`.report_tables.udsp_etl_revenue_materialization_opps();--ok

--Typex=OPPORTUNITY_TYPE, core_account_number=0,FINAL_OPPORTUNITY_TYPE=Typex, CONFIRMED_AMOUNT=0
--	LEAD_PASSED_MONTH=extract(month from Max_Date_Passed)
call `rax-abo-72-dev`.report_tables.udsp_etl_revenue_materialization_revenue_tickets();--Unrecognized name: FINAL_OPPORTUNITY_TYPE ,device_count,core_account_number

call `rax-abo-72-dev`.report_tables.udsp_etl_device_online_oracle_vs_core();

call `rax-abo-72-dev`.report_tables.udsp_create_pm_report_table();--FINAL_OPPORTUNITY_TYPE, SPLIT_CATEGORY, LEAD_PASSED_MONTH, CONFIRMED_AMOUNT, OPPORTUNITY_TYPE

--Typex=OPPORTUNITY_TYPE

call `rax-abo-72-dev`.report_tables.udsp_etl_dim_contact_info_current();--ok

end;
