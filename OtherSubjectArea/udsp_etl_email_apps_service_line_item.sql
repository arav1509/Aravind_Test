create or replace procedure `rax-abo-72-dev`.sales.udsp_etl_email_apps_service_line_item(v_date date)
-----------------------------------------------------------------------------------------------------------------------------------------------------------
begin
declare currentmonthyear  datetime;
declare currenttime_month int64;
declare workdays int64;
declare caldays int64;
-----------------------------------------------------------------------------------------------------------------------------------------------------------
set currentmonthyear=cast(v_date as datetime);
set currenttime_month=`rax-abo-72-dev`.bq_functions.udf_yearmonth_nohyphen(currentmonthyear);
---------------------------------------------------------------------------------------------------------------------------------------------------
delete from `rax-abo-72-dev`.sales.email_apps_service_line_item where cast(invoice_time_month_key as int64)=currenttime_month;
---------------------------------------------------------------------------------------------------------------------------------------------------
insert into `rax-abo-72-dev`.sales.email_apps_service_line_item
select 
	accountnumber												as cloud_account, 
	serviceid													as serviceid,
	service_name,
	invoicedate													as invoice_date, 
	cast(`rax-abo-72-dev`.bq_functions.udf_yearmonth_nohyphen(cast(invoicedate as date)) as string)		as invoice_time_month_key,
	sum(itemtotal)												as invoice_amount

from 
    `rax-abo-72-dev`.mailtrust.invoicelineitems a 
inner join
    `rax-abo-72-dev`.mailtrust.ref_services b 
on a.serviceid=b.service_id
where 
	`rax-abo-72-dev`.bq_functions.udf_yearmonth_nohyphen(cast(invoicedate as date)) =currenttime_month
group by
    accountnumber, 
    serviceid,
    service_name,
    invoicedate, 
    `rax-abo-72-dev`.bq_functions.udf_yearmonth_nohyphen(cast(invoicedate as date));

end;