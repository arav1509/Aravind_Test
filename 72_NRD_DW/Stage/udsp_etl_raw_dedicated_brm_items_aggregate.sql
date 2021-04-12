CREATE OR REPLACE PROCEDURE `rax-staging-dev.stage_one.udsp_etl_raw_dedicated_brm_items_aggregate`()
BEGIN

/*Version			Modified By          Date		 Description  
----------------------------------------------------------------------------------------  
1.1				anil2912			07/06/2019	 copied from 72 server as part of NRD	  
*/  
-----------------------------------------------------------------------------------------------------------
create or replace table stage_one.raw_dedicated_brm_items_aggregate as 
SELECT 
    ITEM_BILL_NO,
    Bill_Obj_Id0,
    OPENED_DATE					AS Bill_DATE,
    bq_functions.udf_yearmonth_nohyphen(OPENED_DATE) AS Time_Month_Key,
    AR_BILL_OBJ_ID0,
    ACCOUNT_OBJ_ID0,
    SUM(ITEM_TOTAL)					AS ITEM_TOTAL,
    GL_SEGMENT						AS ITEM_GL_SEGMENT,
	current_datetime() AS loaded_date

FROM
   stage_two_dw.stage_dedicated_brm_items item
WHERE 
    (ifnull(Item.Bill_Obj_Id0,0)+ifnull(Item.AR_BILL_OBJ_ID0,0)) <>0
GROUP BY
    ITEM_BILL_NO,
    Bill_Obj_Id0,	
    OPENED_DATE,
    AR_BILL_OBJ_ID0,
    ACCOUNT_OBJ_ID0,
    GL_SEGMENT;
	
END;
