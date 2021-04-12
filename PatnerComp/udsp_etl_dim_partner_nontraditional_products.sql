CREATE OR REPLACE PROCEDURE `rax-abo-72-dev`.sales.udsp_etl_dim_partner_nontraditional_products()
begin
-------------------------------------------------------------------------------------------------------------
declare v_product_key int64;
set v_product_key= (select ifnull(max(product_key),0)+1  from `rax-abo-72-dev`.sales.dim_partner_nontraditional_products);


create or replace temp table Partner as 
SELECT DISTINCT --INTO  #Partner
       Product_Group,
       Product_Type,
       Product_Type_Charge_Type,
       0 as Partner_Comp,
       cast('AWS' as string) as Partner_Comp_Type,
	   'N/A' as Fastlane_Event_Type
FROM `rax-abo-72-dev`.sales.partner_program_line_item_detail
WHERE upper(Product_Group) = 'AWS'
--AND Product_Type_Charge_Type = 'Support'
----------------
UNION ALL
----------------
SELECT DISTINCT
       Product_Group,
       Product_Type,
       Product_Type_Charge_Type,
       0 as Partner_Comp,
       cast('Azure' as string) as Partner_Comp_Type,
	   'N/A' as Fastlane_Event_Type
FROM  `rax-abo-72-dev`.sales.partner_program_line_item_detail
WHERE upper(Product_Group) = 'AZURE'
----------------
UNION ALL
----------------
SELECT DISTINCT
       Product_Group,
       Product_Type,
       Product_Type_Charge_Type,
       0 as Partner_Comp,
       cast('O365' as string) as Partner_Comp_Type,
	   'N/A' as Fastlane_Event_Type
FROM  `rax-abo-72-dev`.sales.partner_program_line_item_detail
WHERE
          upper(Product_Group) LIKE '%OFFICE%365%'
	   or upper(Product_type) = 'RACKSPACE APPLICATION SERVICES ON PRODUCTIVITY – ARREAR'
	   or upper(Product_type) LIKE 'RACKSPACE APPLICATION SERVICES FOR M365%'
	   or upper(Product_group) LIKE 'MICROSOFT 365 LICENSING%'
----------------
UNION ALL
----------------
SELECT DISTINCT
       Product_Group,
       Product_Type,
       Product_Type_Charge_Type,
       0 as Partner_Comp,
       cast('Google' as string) as Partner_Comp_Type,
	   'N/A' as Fastlane_Event_Type
FROM  `rax-abo-72-dev`.sales.partner_program_line_item_detail
WHERE  upper(Product_Group) like '%GOOGLE%' or  upper(Product_Group) LIKE '%MGCP%'
----------------
UNION ALL
----------------
SELECT DISTINCT
       Product_Group,
       Product_Type,
       Product_Type_Charge_Type,
       0 as Partner_Comp,
       cast('VMWare' as string) as Partner_Comp_Type,
	   'N/A' as Fastlane_Event_Type
FROM  `rax-abo-72-dev`.sales.partner_program_line_item_detail
WHERE upper(Product_type) LIKE '%VMWARE%CLOUD%'
----------------
UNION ALL
----------------
SELECT DISTINCT
       Product_Group,
       Product_Type,
       Product_Type_Charge_Type,
       0 as Partner_Comp,
       cast('ProServ' as string) as Partner_Comp_Type,
	   Fastlane_Event_Type
FROM  `rax-abo-72-dev`.sales.partner_program_line_item_detail
WHERE upper(Product_Group)LIKE '%PROFESSIONAL%SERVICE%'
----------------
UNION ALL
----------------
SELECT DISTINCT
       Product_Group,
       Product_Type,
       Product_Type_Charge_Type,
       0 as Partner_Comp,
       cast('Colo' as string) as Partner_Comp_Type,
	   'N/A' as Fastlane_Event_Type
FROM  `rax-abo-72-dev`.sales.partner_program_line_item_detail
WHERE upper(Product_Group) LIKE '%COLOCATION%'
----------------
UNION ALL
----------------
SELECT DISTINCT
       Product_Group,
       Product_Type,
       Product_Type_Charge_Type,
       0 as Partner_Comp,
       cast('Armor' as string) as Partner_Comp_Type,
	   'N/A' as Fastlane_Event_Type
FROM  `rax-abo-72-dev`.sales.partner_program_line_item_detail
WHERE upper(Product_Group)LIKE '%ARMOR%';
-------------------------------------------------------------------------------------------------------------
-------------------------------------------------------------------------------------------------------------
INSERT INTO  `rax-abo-72-dev`.sales.dim_partner_nontraditional_products
	(
	  Product_key,
	  Product_Group,
      Product_Type,
      Charge_Type,
      Partner_Comp,
      Partner_Comp_Type,
      Updated,
	  Fastlane_Event_Type
	)
SELECT DISTINCT
	  v_product_key,
	  Product_Group,
      Product_Type,
      Product_Type_Charge_Type as Charge_Type,
      Partner_Comp,
      Partner_Comp_Type,
      0 as Updated,
	  Fastlane_Event_Type

FROM Partner
WHERE
	concat(Product_Group,'_',Product_type,'_',Product_Type_Charge_Type,'_',ifnull(Fastlane_Event_Type,'0') )not in (select distinct concat(Product_Group,'_',Product_type,'_',Product_Type_Charge_Type,'_',ifnull(Fastlane_Event_Type,'0')) from `rax-abo-72-dev`.sales.dim_partner_nontraditional_products
)
		;
end;
