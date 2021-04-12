CREATE OR REPLACE PROCEDURE `rax-staging-dev.stage_two_dw.udsp_update_is_transaction_successful_dedicated`()
BEGIN


	
	declare currentday datetime;
  declare intervals int64;
   declare dedicated_count int64		;
   
   
        declare mail_body_count string;
        declare body_main_dedicated string;
        declare subject_count string;
        declare recipients_count string	 ; 
        
        declare mail_body string;
        declare subject string;
        declare recipients string;
        
        declare subject_failure string;
        declare body string;
        
       	declare toemail string;
        declare profile_name string;
        
  
   set currentday = current_datetime();
   set intervals = 1;
 	
	
	
		
			
		--inserting day-1 changed item bill id's into temp table
	create or replace temporary table temp_current_item_id
              as 
    select distinct item_bill_obj_id0 
		from `rax-staging-dev`.stage_two_dw.stage_dedicated_inv_event_detail
		where etldtt>= current_date() - intervals;
		
		--inserting all the amount for item bill id for aggregated calculation

		create or replace temporary table temp_stage_dedicated AS
		select  s.item_bill_obj_id0, sum(s.total) as total_amount,s.is_transaction_successful
		from `rax-staging-dev.stage_two_dw.stage_dedicated_inv_event_detail` s
		inner join temp_current_item_id t
		on s.item_bill_obj_id0 = t.item_bill_obj_id0
		group by s.item_bill_obj_id0,s.is_transaction_successful;

		--based on aggregated amount calculation for item bill id generating is_transaction_successful column
		create or replace temporary table temp_update_dedicated AS
    Select t2.*
		from
		(
		 select  b.bill_obj_id0, 
		 case when cast(a.total_amount as numeric ) = cast(b.item_total as numeric) then 1 else 0 end as is_transaction_successful
		 from 
			(	select bill_obj_id0, sum(item_total) as item_total 
				from stage_two_dw.stage_dedicated_brm_items
				group by bill_obj_id0
			) b
		 inner join  temp_stage_dedicated  a 
		 on  a.item_bill_obj_id0 = b.bill_obj_id0
		) t2;
		

	

		update temp_stage_dedicated dedicated
		set dedicated.is_transaction_successful = ifnull(x.is_transaction_successful,0)
		from temp_update_dedicated  x
		where
		 dedicated.item_bill_obj_id0 = x.bill_obj_id0;

		update stage_two_dw.stage_dedicated_inv_event_detail ded
		set ded.is_transaction_successful = temp.is_transaction_successful
		from temp_stage_dedicated temp
		where ded.item_bill_obj_id0 = temp.item_bill_obj_id0;
		
	
		select dedicated_count = count(*) from temp_stage_dedicated where is_transaction_successful = 0;
		
		if (dedicated_count > 0)	
       then
     	    
	    set body_main_dedicated = concat('<html><body>', 
		'select </br>',
		'distinct </br>',
	    ' item_bill_obj_id0 </br>',
	    'dedicated as lob </br>',
	    'from stage_two_nrd_deploymenttest.dbo.stage_dedicated_inv_event_detail </br>',
	    'here is_transaction_successful = 0 </br>',
	    '</body></html');
	    --print body
	    
	--select @body_main
		
		set mail_body_count = concat('<html><body><p><font size="3" color = "darkblue">', 'hi team, ','</br></br>' ,'please find below are the queries to find details for transactions which are not completed today for dedicated lob ', '</br></br>', 'body_main_dedicated', '</br>', 'this is a system generated mail. please do not reply to this mail.', '</br></br>', 'thanks','</br>','nrd deveopment team','</font></p></body></html>');
		
		set subject_count = 'nrd staging is_transaction_successful summary for dedicated lob';
		--print @body_output

		set recipients_count = 'rahul.chourasiya@rackspace.com;anil.kumar@rackspace.com;anil.dev@rackspace.com;harish.gowtham@rackspace.com';
		
   CALL `rax-staging-dev.stage_two_dw.sp_send_dbmail`();
		set from_address = 'no_reply@rackspace.com';
		 set body = mail_body_count;
		set body_format ='html';
		set recipients = recipients_count; 
		set subject =  subject_count;
		end if;
		
		if (dedicated_count = 0)
		then
		
	    --print body
	    
	--select @body_main
		
		set mail_body = concat('<html><body>','<p><font',' size="3"',' color = "darkblue">', 'hi team','<br><br>' ,'all the transactions (is_successful_transaction) marked successfully for todays load for dedicated lob' ,'<br>', 'this is a system generated mail. please do not reply to this mail.', '</br></br>', 'thanks','<br>','nrd deveopment team','<font><p><body><html>');
		
		set subject = 'nrd staging is_transaction_successful summary for dedicated lob';
		--print @body_output

		set recipients = 'rahul.chourasiya@rackspace.com;anil.kumar@rackspace.com;anil.dev@rackspace.com;harish.gowtham@rackspace.com';
		
   CALL `rax-staging-dev.stage_two_dw.sp_send_dbmail`();
		--exec msdb.dbo.sp_send_dbmail
		set from_address = 'no_reply@rackspace.com';
		set body = mail_body;
		set body_format ='html';
		set recipients = recipients;
		set subject =  subject;
		
		end if;



set subject_failure = 'nrd staging is transaction successful dedicated lob failure notification';

set body = concat('data transformation failed during fact table load' 
	--, chr(10) , chr(13) , 'error number:  ' , cast(error_number() as string)
	--, chr(10) , chr(13) , 'error severity:  ' , cast(error_severity() as string)
	--, chr(10) , chr(13) , 'error state:  ' , cast(error_state() as string)
--, chr(10) , chr(13) , 'error procedure:  ' , cast(error_procedure() as string)
--, chr(10) , chr(13) , 'error line:  ' , cast(error_line() as string)
--, chr(10) , chr(13) , 'error message: ' , error_message()
, chr(10) , chr(13) , chr(10) , chr(13) , chr(10) , chr(13) , chr(10) , chr(13) , 'this is a system generated mail. do not reply ');

  set toemail = 'rahul.chourasiya@rackspace.com;anil.dev@rackspace.com';

  set profile_name = 'jobs';
   
--CALL `rax-staging-dev.stage_two_dw.sp_send_dbmail`();
	-----exec msdb.dbo.sp_send_dbmail profile_name = profile_name,
	--recipients = toemail, subject = subject_failure, body = body;



end;
