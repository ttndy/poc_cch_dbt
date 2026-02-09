SELECT 
     BASE.CATEGORYGROUP_DESC         AS "category_group"
    ,BASE.MATERIALTYPE_DESC          AS "material_type"
    ,BASE.FORMOFPOWER_DESC           AS "form_of_power"
    ,BASE.ACCOUNT                    AS "pricing_form_account"
    ,BASE.DRIVER                     AS "driver"
    ,BASE.VALUE                      AS "value"
    ,BASE.SGA_BUDGET_CAT             AS "sga_budget_account_refrence"
    ,BASE.DATE_UPDATED               AS "updated_as_of_date"
    ,BASE.SCENARIO_YEAR              AS "scenario"
FROM {{source('pricing_form', 'src_sga_rates')}} AS BASE 
