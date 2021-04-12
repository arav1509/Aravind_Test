CREATE OR REPLACE PROCEDURE `rax-staging-dev.stage_one.udsp_etl_raw_cloud_brm_items_aggregate`()
BEGIN
---------------------------------------------------------------------------------------------------------------------  

---1677240  
CREATE OR REPLACE TABLE      stage_one.raw_cloud_brm_items_aggregate  AS
SELECT   
    ITEM_BILL_NO,  
    Bill_Obj_Id0,  
    OPENED_DATE     AS Bill_DATE,  
    CAST(OPENED_DATE as STRING) AS Time_Month_Key,  
    AR_BILL_OBJ_ID0,  
    ACCOUNT_OBJ_ID0,  
    SUM(ITEM_TOTAL)     AS ITEM_TOTAL,  
    GL_SEGMENT      AS ITEM_GL_SEGMENT,
	current_date() AS loaded_date ---1.1 
 
FROM  
   stage_two_dw.stage_cloud_brm_items Item
WHERE   
    (IFNULL(Item.Bill_Obj_Id0,0)+IFNULL(Item.AR_BILL_OBJ_ID0,0)) <>0  
GROUP BY  
    ITEM_BILL_NO,  
    Bill_Obj_Id0,   
    OPENED_DATE,  
    AR_BILL_OBJ_ID0,  
    ACCOUNT_OBJ_ID0,  
    GL_SEGMENT;  

--------------------------------------------------------------------------------------------------------------------- 

END;
