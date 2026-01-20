-- update from last statement: 
-- 1. Hardcode 'GROSSSALES$' as driver 
-- 2. Use avg value (Sam's logic) for materials that don't have matching identities in PRODUCTS TABLES
SELECT 
         UPLOAD."material"                                          AS "material"
        ,UPLOAD."material_description"                              AS "material_description"
        ,UPLOAD."submisison_type"                                   AS "submisison_type"
        ,UPLOAD."customer"                                          AS "customer"
        ,UPLOAD."department"                                        AS "department"
        ,UPLOAD."new_invoice"                                       AS "new_invoice"
        ,UPLOAD."new_retail"                                        AS "new_retail"
        ,UPLOAD."outdoor_standard_cost"                             AS "standard_cost"
        ,WARRANTY."pricing_form_account"                            AS "pricing_form_account"
        ,ABS(COALESCE(WARRANTY."value", WARRANTY_AVG."value"))      AS "rate"
        ,'GROSSSALES$'                                              AS "driver"
        ,UPLOAD."scenario_year"                                     AS "rate_scenario"
        ,UPLOAD."material_overhead_cost"                    AS "mat_oh_cost"
        
    FROM {{ ref('stg_pricing_form__pl_test') }} AS UPLOAD
        CROSS JOIN (
            SELECT DISTINCT 
                "pricing_form_account"
            FROM {{ ref('stg_pricing_form__warranty_rates')}} 
        ) AS ACC
        LEFT JOIN {{ ref('stg_pricing_form__warranty_rates')}} AS WARRANTY
            ON UPLOAD."customer" = WARRANTY."customer_group"
            AND UPLOAD."material_type" = WARRANTY."material_type"
            AND UPLOAD."form_of_power" = WARRANTY."form_of_power"
            AND UPLOAD."category_group" = WARRANTY."category_group"
            AND UPLOAD."sub_category" = WARRANTY."sub_category"
            AND UPLOAD."power_description" = WARRANTY."power_description"
            AND UPLOAD."return_handling_type" = WARRANTY."return_handling_type"
            AND ACC."pricing_form_account" = WARRANTY."pricing_form_account"
            AND UPLOAD."scenario_year" = WARRANTY."scenario_year"
        LEFT JOIN {{ ref('stg_pricing_form__warranty_rates')}} AS WARRANTY_AVG
            ON (UPLOAD."return_handling_type" || 'Average') = WARRANTY_AVG."return_handling_type"
            AND ACC."pricing_form_account" = WARRANTY_AVG."pricing_form_account"
            AND UPLOAD."scenario_year" = WARRANTY_AVG."scenario_year"
