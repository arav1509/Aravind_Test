CREATE OR REPLACE PROCEDURE `rax-abo-72-dev`.report_tables.udsp_send_alert_new_support_team_added()
begin


declare value int64;
DECLARE msg string;
declare body STRING;
declare Team string;
declare Country string;
declare Region string;
declare SubRegion string;
declare Segment string;
declare RSegment string;
declare SSegment string;
declare BU string;
declare FBU string;
declare Oracle string;
declare ci int64;
declare max  int64;
SET value =	
		(
		SELECT COUNT(*)
		
		FROM 
			`rax-abo-72-dev`.report_tables.dim_support_team_hierarchy A
		WHERE
			Country is null OR Region is null OR Sub_Region is null OR Team_Business_Segment is null OR Team_Business_Sub_Segment is null OR Team_Business_Unit is null
		);


IF 	value = 0 then
	SET msg = '';
     select msg;

ELSeIF value > 0 then

	-- get today's report_tables_duration_log entries
		CREATE OR REPLACE TEMP TABLE TEMP_TABLE as
		select 
        ROW_NUMBER() OVER() as table_id,
		Team_Name, 
		ifnull(Country,'NULL') as Country,
		ifnull(Region,'NULL') as Region,
		ifnull(Sub_Region,'NULL') as Sub_Region, 
		ifnull(Team_Business_Segment,'NULL') as Team_Business_Segment,
		ifnull(Team_Reporting_Segment,'NULL') as Team_Reporting_Segment,
		ifnull(Team_Business_Sub_Segment,'NULL') as Team_Business_Sub_Segment,
		ifnull(Team_Business_Unit,'NULL') as Team_Business_Unit,
		ifnull(Finance_Business_Unit,'NULL') as Finance_Business_Unit,
		Oracle_Team_ID  
		FROM `rax-abo-72-dev`.report_tables.dim_support_team_hierarchy
		WHERE
			Country is null OR Region is null OR Sub_Region is null OR Team_Business_Segment is null OR Team_Business_Sub_Segment is null OR Team_Business_Unit is null;
		
	-- build HTML body header

	set body = '<h2>New Team_Name added to 480072.Report_Tables.dbo.Support_Team_Heirarchy please update Team </h2><table border=1><tr><th><strong>Team</strong></th><th><strong>Country</strong></th><th><strong>Region</strong></th><th><strong>Sub_Region</strong></th><th><strong>Team_Business_Segment</strong></th><th><strong>Team_Reporting_Segment</strong></th><th><strong>Team_Business_Sub_Segment</strong></th><th><strong>Team_Business_Unit</strong></th><th><strong>Finance_Business_Unit</strong></th><th><strong>Oracle_Team_ID</strong></th></tr>';


	select  max(table_id) as max from TEMP_TABLE;
	set ci = 1 ;

	-- loop through duration log and add to email body
	while (ci <= max)
	do
		SET(Team, Country, Region, SubRegion, Segment, RSegment, SSegment, BU , FBU, Oracle)=
		(select AS STRUCT Team_Name , Country , Region , Sub_Region , Team_Business_Segment , Team_Reporting_Segment , Team_Business_Sub_Segment, Team_Business_Unit , Finance_Business_Unit , Oracle_Team_ID
		from TEMP_TABLE where table_id = ci
		);
		set body = concat(body , '<tr><td>',Team,'</td><td>',Country,'</td><td>',Region,'</td><td>',SubRegion,'</td><td>',Segment,'</td><td>',RSegment,'</td><td>',SSegment,'</td><td>',BU,'</td><td>',FBU,'</td><td>',Oracle,'</td></tr>');
		set ci = ci + 1;
	end while;

	set body = concat(body , '</table>');
  select body;
  RAISE USING MESSAGE = concat('<h3 style="background-color:DodgerBlue;">',body	);
	--exec msdb.dbo.Usp_send_cdosysmail
	--		  From='JobSuccessfulNetRevenue.com',
	--          recipients=To,
	--          Subject= Job_Subject,
	--          Body=body,
	--          body_format = 'HTML'

	/*exec msdb.dbo.sp_send_dbmail
		  --From='JobSuccessfulNetRevenue.com',
          recipients=	 'data_architectsrackspace.com;biljana.jovanovarackspace.com',
          Subject= 'New Support Team',
          Body=body,
          body_format = 'HTML'
		  */
end if;	
end;