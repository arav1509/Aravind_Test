CREATE OR REPLACE PROCEDURE `rax-staging-dev.stage_two_dw.udsp_etl_stage_payment_term`()
BEGIN

-------------------------------------------------------------------------------------------------------------------  
SELECT  --INTO     #Dim_Payment_Term  
    PAYMENT_TERM,   
    CAST('Unknown' as string) AS PAYMENT_TERM_DESC  
FROM (  
select DISTINCT  
     pi.PAYMENT_TERM  
from   
    `rax-landing-qa`.brm_ods.account_t a   
INNER JOIN      
   `rax-landing-qa`.brm_ods.bal_grp_t bg    
ON a.BAL_GRP_OBJ_ID0= bg.POID_ID0    
INNER JOIN     
    `rax-landing-qa`.brm_ods.billinfo_t bi   
ON bg.BILLINFO_OBJ_ID0 = bi.POID_ID0     
INNER JOIN       
    `rax-landing-qa`.brm_ods.payinfo_t pi   
ON a.BAL_GRP_OBJ_ID0 = bg.POID_ID0      
INNER JOIN        
    `rax-landing-qa`.brm_ods.billinfo_t bip   
ON bi.AR_BILLINFO_OBJ_ID0 = bip.POID_ID0  
and bip.PAYINFO_OBJ_ID0 = pi.POID_ID0  
Order By  PAYMENT_TERM)  ;
---------------------------------
INSERT INTO  
 stage_two_dw.stage_payment_term  
 (  
 PAYMENT_TERM, PAYMENT_TERM_DESC  
 )  
SELECT DISTINCT  
 PAYMENT_TERM,   
 PAYMENT_TERM_DESC  
FROM   
(
		SELECT  --INTO     #Dim_Payment_Term  
			PAYMENT_TERM,   
			CAST('Unknown' as string) AS PAYMENT_TERM_DESC  
		FROM (  
		select DISTINCT  
			 pi.PAYMENT_TERM  
		from   
			`rax-landing-qa`.brm_ods.account_t a   
		INNER JOIN      
		   `rax-landing-qa`.brm_ods.bal_grp_t bg    
		ON a.BAL_GRP_OBJ_ID0= bg.POID_ID0    
		INNER JOIN     
			`rax-landing-qa`.brm_ods.billinfo_t bi   
		ON bg.BILLINFO_OBJ_ID0 = bi.POID_ID0     
		INNER JOIN       
			`rax-landing-qa`.brm_ods.payinfo_t pi   
		ON a.BAL_GRP_OBJ_ID0 = bg.POID_ID0      
		INNER JOIN        
			`rax-landing-qa`.brm_ods.billinfo_t bip   
		ON bi.AR_BILLINFO_OBJ_ID0 = bip.POID_ID0  
		and bip.PAYINFO_OBJ_ID0 = pi.POID_ID0  
		Order By  PAYMENT_TERM)
)--#Dim_Payment_Term  
WHERE   
 PAYMENT_TERM NOT IN  
  (  
  SELECT DISTINCT   
   PAYMENT_TERM  
  FROM  
   stage_two_dw.stage_payment_term  
  )  ;
  
  END;
