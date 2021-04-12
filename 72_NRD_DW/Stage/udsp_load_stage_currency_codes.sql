CREATE OR REPLACE PROCEDURE `rax-staging-dev.stage_two_dw.udsp_load_stage_currency_codes`()
BEGIN  


DECLARE v_lc_newbrmcode INT64;
DECLARE v_lc_currency_record_id int64;
DECLARE JobName STRING; 
DECLARE body STRING;
DECLARE v_lc_updateexchg INT64;
DECLARE v_lc_modifiedbrm INT64;




set v_lc_newbrmcode=(
select count (*) 
from (
		select    
		b.*    --into #newbrmcode    
		from  (
			select distinct currency_id, currency, name --into #brmcodes 
			from `rax-landing-qa`.brm_ods.ifw_currency 
		)b --#brmcodes b    
		left outer join    
		 stage_two_dw.stage_currency_codes d    
		 on b.currency_id = d.currency_id    
		 where d.currency_id is null  
)--#newbrmcode
);


 --anything NEW?:    
    
    
IF v_lc_newbrmcode = 0   THEN 
    
SELECT 'no new brm codes' ;--PRINT 'no new brm codes';    
-----------    
-- add any new brm codes to dimension:    
ELSE    
 
set v_lc_currency_record_id= (
select ifnull(max(currency_record_id),0)+1 from  stage_two_dw.stage_currency_codes 
); 
SELECT 'new brm codes added to table';    
INSERT INTO  stage_two_dw.stage_currency_codes    
SELECT  v_lc_currency_record_id  
		, cast(b.currency_id as int64) AS Currency_ID    
      ,b.currency  AS Currency    
      ,b.name AS Currency_Description    
      ,b.currency AS BRM_Currency_Abbreviation    
      ,NULL AS Oracle_Exchange_Currency_Code    
      ,NULL AS Oracle_Exchange_Currency_Name    
      ,CURRENT_DATETIME() AS initial_record_load_dtt    
      ,CURRENT_DATETIME() AS record_modify_dtt    
FROM     
(
		select    
		b.*    --into #newbrmcode    
		from  (
			select distinct currency_id, currency, name --into #brmcodes 
			from `rax-landing-qa`.brm_ods.ifw_currency 
		)b --#brmcodes b    
		left outer join    
		 stage_two_dw.stage_currency_codes d    
		 on b.currency_id = d.currency_id    
		 where d.currency_id is null  
) b--#newbrmcode b    
;


 
--NOTIFY NEW record ADDED:    
 
 SET JobName = 'Slicehost Stage_Currency_Codes -new value'    ;
 SET Body = 'New Currency record Added To Slicehost.dbo.Stage_Currency_Codes' ;   
 select JobName,Body;
   /*  
 EXEC msdb..Usp_send_cdosysmail 'NewCurrency@CloudUsage.com',     
   'kcannick@rackspace.com;david.alvarez@rackspace.com;lmarshal@rackspace.com;jc.mcconnell@rackspace.com'    
   --'anil.kumar@rackspace.com'  
    ,@JobName,@Body     
;    
 
    */
   
end if;
    
  
--is info avail in exchange:    
SET v_lc_updateexchg =( SELECT COUNT(*) FROM (select    --into #updateexchg
m.Currency_ID    
      ,m.Currency    
      ,m.Currency_Description    
      ,m.BRM_Currency_Abbreviation    
      ,o.Exchange_Rate_From_Currency_Code    
      ,o.Exchange_Rate_From_Currency_Description    
from    
(
		SELECT    --into #missingExchg 
		Currency_ID    
		,Currency    
		,Currency_Description    
		,BRM_Currency_Abbreviation    
		from     
			stage_two_dw.stage_currency_codes    
		  where Oracle_Exchange_Currency_Code is null    
)m --#missingExchg m    
inner join    
(
		SELECT    --  into #oraclecodes 
			  Exchange_Rate_From_Currency_Code    
			  ,Exchange_Rate_From_Currency_Description    
			
		  FROM stage_one.raw_report_exchange_rate    
		  where       upper(Exchange_Rate_To_Currency_Code) = 'USD'    
		  group by       Exchange_Rate_From_Currency_Code    
			  ,Exchange_Rate_From_Currency_Description  
) o--#oraclecodes o    
on m.Currency = o.Exchange_Rate_From_Currency_Code    
));    
--anything to add?    
    
IF v_lc_updateexchg = 0    THEN

SELECT 'no new exchange records';  
--print 'no new exchange records';    
    
-----------------    
ELSE    
--update records with Exchange data:    
    
UPDATE      stage_two_dw.stage_currency_codes D  
SET    
     Oracle_Exchange_Currency_Code = Exchange_Rate_From_Currency_Code    
      ,Oracle_Exchange_Currency_Name = Exchange_Rate_From_Currency_Description    
      ,record_modify_dtt = CURRENT_DATETIME()        
       
from   
  (
		(select    --into #updateexchg
		m.Currency_ID    
			  ,m.Currency    
			  ,m.Currency_Description    
			  ,m.BRM_Currency_Abbreviation    
			  ,o.Exchange_Rate_From_Currency_Code    
			  ,o.Exchange_Rate_From_Currency_Description    
		from    
		(
				SELECT    --into #missingExchg 
				Currency_ID    
				,Currency    
				,Currency_Description    
				,BRM_Currency_Abbreviation    
				from     
					stage_two_dw.stage_currency_codes    
				  where Oracle_Exchange_Currency_Code is null    
		)m --#missingExchg m    
		inner join    
		(
				SELECT    --  into #oraclecodes 
					  Exchange_Rate_From_Currency_Code    
					  ,Exchange_Rate_From_Currency_Description    
					
				  FROM stage_one.raw_report_exchange_rate    
				  where       upper(Exchange_Rate_To_Currency_Code) = 'USD'    
				  group by       Exchange_Rate_From_Currency_Code    
					  ,Exchange_Rate_From_Currency_Description  
		) o--#oraclecodes o    
		on m.Currency = o.Exchange_Rate_From_Currency_Code    
		)
  )  u --#updateexchg u    
  where     
    d.Oracle_Exchange_Currency_Code IS NULL    
 AND d.Currency_ID = u.Currency_ID    
 AND d.Currency = u.Currency    
    AND d.Currency_Description = u.Currency_Description    
    AND d.BRM_Currency_Abbreviation = u.BRM_Currency_Abbreviation    
;   

 
--NOTIFY  record UPDATED:    
 --declare @JobName varchar(max)     
 --declare @body varchar(MAX)    
 SET JobName = 'Slicehost Stage_Currency_Codes -updated Exchange value'    ;
 SET Body = 'Currency record updated with Oracle Exchange data in Cloud_Usage.dbo.Stage_Currency_Codes'    ;
 select JobName,Body;
     /*
 EXEC msdb..Usp_send_cdosysmail 'NewCurrency@CloudUsage.com',     
   'kcannick@rackspace.com;david.alvarez@rackspace.com;lmarshal@rackspace.com;jc.mcconnell@rackspace.com'  
   --'anil.kumar@rackspace.com'  
    ,@JobName,@Body     
;    
 */
    
    
  end if; 
    

    
/***** UPDATE BRM RECORD VALUES  *****/    
--identify when a BRM record is modified (code or description)    
    
   


set v_lc_modifiedbrm=(select count(*) from (
SELECT     --into #modifiedbrm 
a.Currency_ID    
,a.Currency    
,a.Currency_Description    
,a.BRM_Currency_Abbreviation    
,b.CURRENCY as new_brm_currency_abbrev    
,b.NAME as new_brm_currency_descr     
  FROM     
`rax-landing-qa`.brm_ods.ifw_currency b     
left outer join    
 stage_two_dw.stage_currency_codes a    
 on a.Currency_ID = b.Currency_ID    
WHERE     
a.BRM_Currency_Abbreviation <> b.CURRENCY    
OR a.Currency_Description <> b.NAME    
));

    
IF v_lc_modifiedbrm = 0    then

--print 'no modified brm records'    
select 'no modified brm records' ;
    
--------------    
ELSE    
    
--update modified value    
UPDATE      stage_two_dw.stage_currency_codes D    
SET    
    BRM_Currency_Abbreviation = new_brm_currency_abbrev    
,Currency_Description = new_brm_currency_descr    
FROM      
 (
		SELECT     --into #modifiedbrm 
		a.Currency_ID    
		,a.Currency    
		,a.Currency_Description    
		,a.BRM_Currency_Abbreviation    
		,b.CURRENCY as new_brm_currency_abbrev    
		,b.NAME as new_brm_currency_descr     
		  FROM     
		`rax-landing-qa`.brm_ods.dbo.ifw_currency b     
		left outer join    
		 stage_two_dw.stage_currency_codes a    
		 on a.Currency_ID = b.Currency_ID    
		WHERE     
		a.BRM_Currency_Abbreviation <> b.CURRENCY    
		OR a.Currency_Description <> b.NAME    
 ) m--#modifiedbrm m    
 where d.Currency_ID = m.Currency_ID   ; 
    
 --NOTIFY  record MODIFIED:    
 --declare @JobName varchar(max)     
 --declare @body varchar(MAX)    
 SET JobName = 'Slicehost Stage_Currency_Codes -modified BRM value'    ;
 SET Body = 'Currency ID BRM attributes modified in Cloud_Usage.dbo.Stage_Currency_Codes table'  ;  
    select 	JobName,Body ;
	
	/*
 EXEC msdb..Usp_send_cdosysmail 'NewCurrency@CloudUsage.com',     
   'kcannick@rackspace.com;david.alvarez@rackspace.com;lmarshal@rackspace.com;jc.mcconnell@rackspace.com'    
   --'anil.kumar@rackspace.com'  
    ,@JobName,@Body     
  */
 ---------    
  end if; 
    
    
    
    
    
END;
