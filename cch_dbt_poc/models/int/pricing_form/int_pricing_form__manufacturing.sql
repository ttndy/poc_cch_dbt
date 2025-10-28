SELECT 
     UPLOAD."material"                                  AS "material"
    ,UPLOAD."material_description"                      AS "material_description"
    ,UPLOAD."submisison_type"                           AS "submisison_type"
    ,UPLOAD."customer"                                  AS "customer"
    ,UPLOAD."department"                                AS "department"
    ,UPLOAD."new_invoice"                               AS "new_invoice"
    ,UPLOAD."new_retail"                                AS "new_retail"
    ,UPLOAD."outdoor_standard_cost"                     AS "standard_cost"
    ,MFG."pricing_form_account"                         AS "pricing_form_account"
    ,MFG."value"                                        AS "rate"
    ,'To be Added'                                      AS "driver"
    ,MFG."scenario"                                     AS "rate_scenario"
FROM {{ ref('stg_pricing_form__pl_test') }} AS UPLOAD
    LEFT JOIN {{ ref('stg_pricing_form__manufacturing_rates')}} AS MFG
        ON UPLOAD."manufacturing_source"  = MFG."manufacturing_source"
        AND (
            UPLOAD."shipping_terms" = MFG."shipping_terms"
            OR  MFG."shipping_terms" = 'Blank'
        )
