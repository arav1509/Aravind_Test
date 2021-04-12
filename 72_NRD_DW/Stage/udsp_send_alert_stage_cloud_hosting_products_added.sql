CREATE OR REPLACE PROCEDURE `rax-staging-dev.stage_two_dw.udsp_send_alert_stage_cloud_hosting_products_added`()
BEGIN
/*=============================================
-- Author:		hari4586
-- Create date: 11.06.2019
-- Description:	NOTIFICATION to TEAM of New PRODUCT in Stage_Cloud_Hosting_Products table

-- =============================================*/

/*=====================================================
NOTIFICATION to TEAM of New PRODUCT in table
Updated: 
11.18.2014jcm_updated to include new product in htnl chart in message; updated new prod trigger to be group descr 'N/A'
06.29.1015jcm_updated message query to include charge_type = 'n/a'  --was present in the if logic, missing from the message delivery logic
11.05.15jcm_added trello card maint64 alert
04.08.16	jcm	added rajani to team list; removed trello alert
04.13.16	jcm	modified contact names for finding new product groups; changed email to use team dl

=====================================================*/


declare value int64;
DECLARE msg STRING;
declare body string;
declare Product_ID_NK string;
declare Product_Name  string ;
declare Product_Reporting_Group_DESC string ;
declare Is_Stand_Alone_Fee string ;
declare Normalize string ;
declare Charge_Type string ;
declare Is_Global_Net_Revenue string ;
declare Is_Cloud_Comp_Revenue string ;
declare ci int64;
declare max int64 ;
declare s_no int64;


SET value =	
	(
		SELECT DISTINCT
			COUNT(*)
		FROM 
			stage_two_dw.stage_cloud_hosting_products A
		WHERE
		 	--Product_Reporting_Group_ID =0
		( UPPER(Product_Reporting_Group_DESC) = 'N/A' OR UPPER(Charge_Type)  = 'N/A')
		);

IF 
	value = 0 THEN 

	SELECT '' AS MSG;

ELSEIF value > 0 THEN 

---------------------------------------------------
-- send notification of new product found
---------------------------------------------------
	BEGIN
	set body = 
		concat('<font face="Arial Narrow">',
		'<h2>New BRM Product added to stage_two_dw.dbo.Stage_Cloud_Hosting_Products.'  ,
		'Please review new items and contact Thomas McIlheran or Binh Luu in Finance, if necessary, to update fields in table for correct reporting values.',
		 '</h2><table border=1><tr>',
      '<th><strong>S.No</strong></th>',
		 '<th><strong>Product_ID_NK</strong></th>',
		 '<th><strong>Product_Name</strong> </th>',
		 '<th><strong>Product_Reporting_Group_DESC</strong></th>',
		 '<th><strong>Is_Stand_Alone_Fee</strong></th>',
		 '<th><strong>Normalize</strong></th>',
		 '<th><strong>Charge_Type</strong></th>',
		 '<th><strong>Is_Global_Net_Revenue</strong></th>',
		 '<th><strong>Is_Cloud_Comp_Revenue</strong></th>',
		 '</tr></font>'
		 );
     
create or replace TEMPORARY  table table_temp as 
			select 
			row_number() over() as table_id,
			Product_ID_NK,
			Product_Name,
			IFNULL(Product_Reporting_Group_DESC, 'NULL') as Product_Reporting_Group_DESC,
			Is_Stand_Alone_Fee,
			Normalize,
			IFNULL(   Charge_Type, 'NULL') as Charge_Type,
			Is_Global_Net_Revenue,
			Is_Cloud_Comp_Revenue
			from  stage_two_dw.stage_cloud_hosting_products
			where 
			UPPER(Product_Reporting_Group_DESC) = 'N/A'  OR UPPER(Charge_Type)  = 'N/A';
    
    set max = (select max(table_id) from table_temp);
		set ci = 1;
      
   while (ci <= max) do
		
		set (s_no,Product_ID_NK,Product_Name,Product_Reporting_Group_DESC,Is_Stand_Alone_Fee,Normalize,Charge_Type,Is_Global_Net_Revenue,Is_Cloud_Comp_Revenue) =
			(select  AS STRUCT
      table_id,
			CAST(Product_ID_NK AS STRING) AS Product_ID_NK,
			CAST(Product_Name AS STRING) AS Product_Name,
			CAST(Product_Reporting_Group_DESC AS STRING) AS Product_Reporting_Group_DESC,
			CAST(Is_Stand_Alone_Fee AS STRING) AS Is_Stand_Alone_Fee,
			CAST(Normalize AS STRING)AS Normalize,
			CAST(Charge_Type AS STRING) AS Charge_Type,
			CAST(Is_Global_Net_Revenue AS STRING)AS Is_Global_Net_Revenue ,
			CAST(Is_Cloud_Comp_Revenue AS STRING) AS Is_Cloud_Comp_Revenue
			from table_temp
			where table_id = ci);

			--set body with table values:
			set body =concat(
			body , '<tr><td>'
      ,s_no,'</td>	<td>'
			,Product_ID_NK,'</td>	<td>'
			,Product_Name,'</td><td>'
			,Product_Reporting_Group_DESC,'</td><td>'
			,Is_Stand_Alone_Fee,'</td><td>'
			,Normalize,'</td><td>'
			,Charge_Type,'</td><td>'
			,Is_Global_Net_Revenue,'</td><td>'
			,Is_Cloud_Comp_Revenue,'</td></tr>'
			)
			;
			set ci = ci + 1;
			SELECT body;
		end while;  
      set body = CONCAT(body , '</table>');
      
      select body;
      end;
END IF;      
/*
	exec msdb.dbo.sp_send_dbmail
			recipients= 'TES_ETL_Supportrackspace.com',
			Subject= 'New Cloud Hosting Products values', --Job_Subject,
			Body=body,
			body_format = 'HTML'
				  ;
				  
--------------------------------------------
/*--create Trello card for maint64enance:	--11.05.15jcm
EXEC Frontend_Updatable_Applications.dbo.udsp_email_trello
 'SalesOps TABLE Slicehost.Dim_Cloud_Hosting_Products NEW product NOTICE'
, srv = 'Sales Ops'
, db = 'Slicehost'*/


END;
