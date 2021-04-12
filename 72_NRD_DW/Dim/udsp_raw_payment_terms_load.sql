CREATE OR REPLACE PROCEDURE `rax-staging-dev.stage_one.udsp_raw_payment_terms_load`()
BEGIN
begin
create or replace table `rax-staging-dev.stage_one.raw_payment_terms` as 

SELECT ltrim(rtrim(cast(REC_ID as STRING))) AS REC_ID
     ,ltrim(rtrim(replace(PAYMENT_TERM_DESC,'_',' '))) AS PAYMENT_TERM_NAME
     ,ltrim(rtrim(PAYMENT_TERM_DESC)) --dbo.initcap(PAYMENT_TERM_DESC)
AS PAYMENT_TERM_DESC
,'BRM' as Source_System_Name
 FROM  `rax-landing-qa.brm_ods.config_payment_term_t`
Union All
SELECT ltrim(rtrim(CAST(Term_Id AS STRING))) AS TERM_ID
     ,ltrim(rtrim(Name)) AS NAME
     ,ltrim(rtrim(Description)) AS DESCRIPTION
,'EBS' as Source_System_Name
FROM `rax-landing-qa.ebs_ods.ra_terms_tl`;

end;
END;
