SELECT 
     BASE.CUSTOMERSUBGROUP           AS "customer_group"
    ,BASE.materialtype_desc          AS "material_type"
    ,BASE.FORMOFPOWER_DESC           AS "form_of_power"
    ,BASE.POWER_DESC                 AS "power_description"
    ,BASE.CATEGORYGROUP_DESC         AS "category_group"
    ,BASE.SUBCATEGORY_DESC           AS "sub_category"
    ,BASE.PRICING_FORM_ACCOUNT_1     AS "pricing_form_account"
    ,BASE.PRICING_FORM_ACCOUNT_2     AS "return_handling_type"
    ,BASE.DRIVER                     AS "driver_account"
    ,BASE.VALUE                      AS "value"
    ,BASE.DATE_UPDATED               AS "updated_as_of_date"
    ,BASE.SCENARIO_YEAR              AS "scenario_year"     
FROM {{source('pricing_form', 'src_warranty_rates')}} AS BASE