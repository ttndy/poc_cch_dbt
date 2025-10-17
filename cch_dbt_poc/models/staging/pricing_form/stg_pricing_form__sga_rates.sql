SELECT 
     BASE.CATEGORYGROUP_DESC         AS "category_group"
    ,BASE.MATERIALTYPE_DESC          AS "material_type"
    ,BASE.FORMOFPOWER_DESC           AS "form_of_power"
    ,BASE.ACCOUNT                    AS "pricing_form_account"
    ,BASE.DRIVER                     AS "test_driver"
    ,BASE.VALUE                      AS "value"
    ,BASE.DATE_UPDATED               AS "updated_as_of_date"
FROM {{source('pricing_form', 'src_sga_rates')}} AS BASE 
