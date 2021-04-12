CREATE OR REPLACE PROCEDURE `rax-staging-dev.stage_three_dw.udsp_merge_dim_payment_terms`()
BEGIN


declare v_payment_term_key int64;
set v_payment_term_key= (select ifnull(max(payment_term_key),0) from `rax-datamart-dev.corporate_dmart.dim_payment_terms`);


MERGE INTO `rax-datamart-dev.corporate_dmart.dim_payment_terms` as TARGET    
USING    
 (    
  SELECT     
  	   ROW_NUMBER() OVER()+v_payment_term_key as payment_term_key,

    Payment_Term_NK as Payment_Term_NK  
      ,InitCap(Payment_Term_Name) AS Payment_Term_Name    
      ,InitCap(Payment_Term_Description) AS Payment_Term_Description   
      ,Source_System_Name    
      ,'SSIS_Dim_Payment_Terms' AS Record_Created_By    
      ,current_timestamp() AS Record_Created_Datetime    
      ,'SSIS_Dim_Payment_Terms' AS Record_Updated_By    
      ,current_timestamp() AS Record_Updated_Datetime     
      ,Chk_Sum_Md5    
   FROM `rax-staging-dev.stage_two_dw.stage_payment_terms`    
 )AS SOURCE    
	ON    
 (       
     TARGET.Payment_Term_Nk = SOURCE.Payment_Term_Nk    
	 and TARGET.Source_SYSTEM_NAME=SOURCE.Source_SYSTEM_NAME
 )    
    
     
 WHEN NOT MATCHED BY TARGET    
 THEN INSERT    
      ( 
	   payment_term_key
       ,Payment_Term_NK  
		,Payment_Term_Name  
		,Payment_Term_Description  
        ,Source_System_Name    
        ,Record_Created_By    
        ,Record_Created_Datetime    
        ,Record_Updated_By    
        ,Record_Updated_Datetime    
        ,Chk_Sum_Md5    
   )    
   Values    
   (    
	  payment_term_key
		,SOURCE.Payment_Term_NK  
	    ,SOURCE.Payment_Term_Name  
        ,SOURCE.Payment_Term_Description  
        ,SOURCE.Source_System_Name    
        ,SOURCE.Record_Created_By    
        ,SOURCE.Record_Created_Datetime    
        ,SOURCE.Record_Updated_By    
        ,SOURCE.Record_Updated_Datetime    
        ,SOURCE.Chk_Sum_Md5
	  )
	  	WHEN MATCHED AND 
	(
  SOURCE.Chk_Sum_Md5 <> TARGET.Chk_Sum_Md5    
	)
	THEN UPDATE
	SET
    TARGET.Payment_Term_Name = SOURCE.Payment_Term_Name,    
   TARGET.Payment_Term_Description = SOURCE.Payment_Term_Description,     
   TARGET.Chk_Sum_Md5 = SOURCE.Chk_Sum_Md5,    
   TARGET.Record_Updated_By = SOURCE.Record_Updated_By,    
   TARGET.Record_Updated_Datetime = current_timestamp();
   	insert `rax-datamart-dev.corporate_dmart.dim_payment_terms`( 
         payment_term_key
       ,Payment_Term_NK  
		,Payment_Term_Name  
		,Payment_Term_Description  
        ,Source_System_Name    
        ,Record_Created_By    
        ,Record_Created_Datetime    
        ,Record_Updated_By    
        ,Record_Updated_Datetime    
        ,Chk_Sum_Md5 
	  )
	  select  
  	   ROW_NUMBER() OVER()+v_payment_term_key as payment_term_key
	  ,SOURCE.Payment_Term_NK  
	    ,SOURCE.Payment_Term_Name  
        ,SOURCE.Payment_Term_Description  
        ,SOURCE.Source_System_Name    
        ,SOURCE.Record_Created_By    
        ,SOURCE.Record_Created_Datetime    
        ,SOURCE.Record_Updated_By    
        ,SOURCE.Record_Updated_Datetime    
        ,SOURCE.Chk_Sum_Md5
		from
	(
	 SELECT     
  	   ROW_NUMBER() OVER()+v_payment_term_key as payment_term_key,

    Payment_Term_NK as Payment_Term_NK  
      ,InitCap(Payment_Term_Name) AS Payment_Term_Name    
      ,InitCap(Payment_Term_Description) AS Payment_Term_Description   
      ,Source_System_Name    
      ,'SSIS_Dim_Payment_Terms' AS Record_Created_By    
      ,current_timestamp() AS Record_Created_Datetime    
      ,'SSIS_Dim_Payment_Terms' AS Record_Updated_By    
      ,current_timestamp() AS Record_Updated_Datetime     
      ,Chk_Sum_Md5    
   FROM `rax-staging-dev.stage_two_dw.stage_payment_terms`) SOURCE    
	inner join `rax-datamart-dev.corporate_dmart.dim_payment_terms` TARGET
	on TARGET.Payment_Term_Nk = SOURCE.Payment_Term_Nk    
	and 
	(
  SOURCE.Chk_Sum_Md5 <> TARGET.Chk_Sum_Md5    
	);
	
   

   END;
