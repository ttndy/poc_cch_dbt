SELECT 
     UPLOAD."material"                                  AS "material"
    ,UPLOAD."material_description"                      AS "material_description"
    ,UPLOAD."customer"                                  AS "customer"
    ,UPLOAD."department"                                AS "department"
    ,UPLOAD."new_invoice"                               AS "new_invoice"
    ,UPLOAD."new_retail"                                AS "new_retail"
    ,UPLOAD."outdoor_standard_cost"                     AS "standard_cost"
    ,BASE_DRIVER."pricing_form_account"                 AS "pricing_form_account"
    ,BASE_DRIVER."value"                                AS "rate"
    ,BASE_DRIVER."driver"                               AS "driver"
FROM {{ ref('stg_pricing_form__pl_test') }} AS UPLOAD
    LEFT JOIN {{ ref('stg_pricing_form__base_drivers')}} AS BASE_DRIVER
        ON UPLOAD."customer" = BASE_DRIVER."customer"
        AND UPLOAD."department" = BASE."department"
