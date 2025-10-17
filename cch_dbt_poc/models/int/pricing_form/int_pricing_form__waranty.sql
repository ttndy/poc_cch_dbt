SELECT 
     UPLOAD."material"                                  AS "material"
    ,UPLOAD."material_description"                      AS "material_description"
    ,UPLOAD."customer"                                  AS "customer"
    ,UPLOAD."department"                                AS "department"
    ,UPLOAD."new_invoice"                               AS "new_invoice"
    ,UPLOAD."new_retail"                                AS "new_retail"
    ,UPLOAD."outdoor_standard_cost"                     AS "standard_cost"
    ,COALESCE(
         WARRANTY."pricing_form_account"
        ,WARRANTY_AVG."pricing_form_account"
    )                                                   AS "pricing_form_account"
    ,COALESCE(
         WARRANTY."value"
        ,WARRANTY_AVG."value"
    )                                                   AS "rate"
    ,'GROSSSAELS$'                                      AS "driver"
FROM {{ ref('stg_pricing_form__pl_test') }} AS UPLOAD
    LEFT JOIN {{ ref('stg_pricing_form__warranty_rates')}} AS WARRANTY
        ON UPLOAD."customer" = WARRANTY."customer_group"
        AND UPLOAD."material_type" = WARRANTY."material_type"
        AND UPLOAD."form_of_power" = WARRANTY."form_of_power"
        AND UPLOAD."category_group" = WARRANTY."category_group"
        AND UPLOAD."sub_categorty" = WARRANTY."sub_categorty"
        AND UPLOAD."power_description" = WARRANTY."power_description"
        AND UPLOAD."return_hadling_type" = WARRANTY."return_hadling_type"  Should be added to upload
    LEFT JOIN {{ ref('stg_pricing_form__warranty_rates')}} AS WARRANTY_AVG
        ON UPLOAD."return_hadling_type" = (WARRANTY_AVG."return_hadling_type" || 'Average')
        AND WARRANTY."pricing_form_account" = WARRANTY_AVG."pricing_form_account"
