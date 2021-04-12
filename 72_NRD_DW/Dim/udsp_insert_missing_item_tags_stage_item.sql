CREATE OR REPLACE PROCEDURE `rax-staging-dev.stage_two_dw.udsp_insert_missing_item_tags_stage_item`()
BEGIN
  
/*Version   Modified By       Date    Description    
----------------------------------------------------------------------------------------    
1.0     RamaMohanReddy(rama4760)   14/10/2019      to insert the item_tags into Stage_Item table   
                  which are available in Xref_Item_Tag table but  
                  not in Dim_Item table     
*/   
  


INSERT INTO stage_two_dw.stage_item  
(Item_Tag,  
Item_Name,  
Item_Description,  
Item_Type,  
Item_Sub_Type,  
Item_Source_Record_Id,  
Record_Created_By,  
Record_Created_Datetime,  
Record_Updated_By,  
Record_Updated_Datetime,  
Chk_Sum_Md5  
)  
select   
Item_Tag,  
Item_Name,  
Item_Description,  
Item_Type,  
Item_Sub_Type,  
-1 as Item_Source_Record_Id,  
'SSIS_Stage_Item' As Record_Created_By,  
current_datetime() AS Record_Created_Datetime,  
'SSIS_Stage_Item' As Record_Updated_By,  
current_datetime() AS Record_Updated_Datetime,  
 TO_BASE64(MD5( CONCAT(Item_Name, '|' ,Item_Description,'|'  
       ,Item_Type,'|',Item_Sub_Type,'|') ) ) AS Chk_Sum_Md5  
FROM 
(
	select   --INSERT INTO #STAGE_ITEM  
	XIT.Item_Tag,  
	REPLACE(XIT.Item_Tag,'_',' ') as Item_Name,  
	REPLACE(XIT.Item_Tag,'_',' ') as Item_Description,  
	XIT.Item_Type,  
	'Unknown' as Item_Sub_Type  
	FROM stage_two_dw.xref_item_tag XIT   
	LEFT JOIN `rax-datamart-dev`.corporate_dmart.dim_item I  
	ON XIT.Item_Tag = I.Item_Tag  
	WHERE I.Item_Tag IS NULL 
) --#STAGE_ITEM  
 ;
  
END;
