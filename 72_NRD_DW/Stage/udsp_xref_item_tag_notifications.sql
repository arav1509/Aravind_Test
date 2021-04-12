CREATE OR REPLACE PROCEDURE `rax-staging-dev.stage_two_dw.udsp_xref_item_tag_notifications`()
BEGIN


----create proc [dbo].[udsp_xref_item_tag_notifications]
---as

----if exists (select 1 from xref_item_tag where item_tag is null)

----begin

declare mail_body string;
declare body_main string;
declare subject string;
declare recipients string;

	
		
		set mail_body = concat('<html><body><p><font size="3">', 'hi team',' ','</br></br>' ,'please find the below data from xref_item_tag which item_tag value is not available', '</br></br>', body_main, '</br>','please go to the following link and correct item_tag value','</br></br>','http://10.10.209.199:82/home.aspx','</br></br>', 'this is a system generated mail. please do not reply to this mail.', '</br></br>', 'thanks','</font></p></body></html>');
		
		set subject = 'item_tag summary';
		
		set recipients = 'get_etl_support@rackspace.com;yoda@rackspace.com;globaldataarchitecture@raxglobal.onmi';
		
		--exec msdb.dbo.sp_send_dbmail
		--@from_address = 'no_reply@rackspace.com',
		----@body = @mail_body,
		--@body_format ='html',
		--@recipients = @recipients, 
		--@subject =  @subject


END;
