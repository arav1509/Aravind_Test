CREATE OR REPLACE PROCEDURE `rax-abo-72-dev`.report_tables.udsp_new_churn_grouping_alert_added()
begin
declare value int64;
DECLARE msg string;
declare body  string;
DECLARE Team STRING;
DECLARE Country STRING;
DECLARE SuperRegion STRING;
DECLARE Region STRING;
DECLARE BU STRING;
DECLARE Segment STRING;
DECLARE Churn_Grouping STRING;
DECLARE CFFlag STRING;
DECLARE Oracle STRING;
DECLARE ci int64;
DECLARE max int64;

SET value =	
		(
		SELECT COUNT(*)
		FROM 
			`rax-abo-72-dev`.report_tables.dim_support_team_hierarchy A
		WHERE
			lower(super_region) ='intl'
		and lower(churn_grouping) = 'unknown' or churn_forecast_flag=99
		and lower(team_name)<>'unknown'	
		);


IF value = 0 then
	SET msg = '';
    select msg;

ELSEIF value > 0 then


		-- get today's report_tables_duration_log entries
		create or replace temp table temp_table as
		select ROW_NUMBER() OVER() as table_id,Team_Name, Country, Super_Region, Region, Team_Business_Unit, Team_Business_Segment, Churn_Grouping, Churn_Forecast_Flag, Oracle_Team_ID  
		from `rax-abo-72-dev`.report_tables.dim_support_team_hierarchy
		WHERE	lower(country) = 'unknown' or lower(super_region)= 'unknown' or lower(region) = 'unknown' or lower(churn_grouping) = 'unknown' or churn_forecast_flag=99
		;

		-- build HTML body header

		set body = '<h2>New International Churn grouping added to 480075.Report_Tables.dbo.Support_Team_Heirarchy_ssl please update Churn_Grouping and Churn_Forecast_Flag </h2><table border=1><tr><th><strong>Team</strong></th><th><strong>Country</strong></th><th><strong>SuperRegion</strong></th><th><strong>Region</strong></th><th><strong>Team_Business_Unit</strong></th><th><strong>Team_Business_Segment</strong></th><th><strong>Churn_Grouping</strong></th><th><strong>Churn_Forecast_Flag</strong></th><th><strong>Oracle_Team_ID</strong></th></tr>'
;

		select max = max(table_id) from temp_table;
		set ci = 1 ;

		-- loop through duration log and add to email body
		while (ci <= max)
		DO
			SET (Team, Country, SuperRegion, Region, BU, Segment, Churn_Grouping,  CFFlag, Oracle)=
			(select  Team_Name , Country,Super_Region , Region , Team_Business_Unit , Team_Business_Segment , Churn_Grouping , Churn_Forecast_Flag , Oracle_Team_ID
			from temp_table where table_id = ci);

			set body = CONCAT(body , '<tr><td>',Team,'</td><td>',Country,'</td><td>',SuperRegion,'</td><td>',Region,'</td><td>',BU,'</td><td>',Segment,'</td><td>',Churn_Grouping,'</td><td>',CFFlag,'</td><td>',Oracle,'</td></tr>');

			set ci = ci + 1;
		END WHILE;
		set body = CONCAT(body , '</table>');
		 RAISE USING MESSAGE = concat('<h3 style="background-color:DodgerBlue;">',body	);
		/*exec msdb.dbo.sp_send_dbmail
				  --From='JobSuccessfulNetRevenue.com',
				  recipients=  'data_architectsrackspace.com ;midhun.jastiRACKSPACE.COM;biljana.jovanovaRACKSPACE.COM;dojedarackspace.com',
				  Subject= 'New Churn Grouping',
				  Body=body,
				  body_format = 'HTML'
				  */
END IF;				  

end;