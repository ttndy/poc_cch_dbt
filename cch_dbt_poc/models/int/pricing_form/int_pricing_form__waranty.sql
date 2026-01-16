SELECT 
     UPLOAD."material"                                  AS "material"
    ,UPLOAD."material_description"                      AS "material_description"
    ,UPLOAD."submisison_type"                           AS "submisison_type"
    ,UPLOAD."customer"                                  AS "customer"
    ,UPLOAD."department"                                AS "department"
    ,UPLOAD."new_invoice"                               AS "new_invoice"
    ,UPLOAD."new_retail"                                AS "new_retail"
    ,UPLOAD."outdoor_standard_cost"                     AS "standard_cost"
    ,WARRANTY."driver_account"                          AS "driver"
    ,COALESCE(
         WARRANTY."pricing_form_account"
        ,WARRANTY_AVG."pricing_form_account"
    )                                                   AS "pricing_form_account"
    ,COALESCE(
         WARRANTY."value"
        ,WARRANTY_AVG."value"
    )                                                   AS "rate"
    ,COALESCE(
         WARRANTY."scenario_year"
        ,WARRANTY_AVG."scenario_year"
    )                                                   AS "rate_scenario"
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
        AND UPLOAD."return_handling_type" = WARRANTY."return_handling_type"  -- Should be added to upload
        AND ACC."pricing_form_account" = WARRANTY."pricing_form_account"
    LEFT JOIN {{ ref('stg_pricing_form__warranty_rates')}} AS WARRANTY_AVG
        ON (UPLOAD."return_handling_type" || 'Average') = WARRANTY_AVG."return_handling_type"
        AND ACC."pricing_form_account" = WARRANTY_AVG."pricing_form_account"
