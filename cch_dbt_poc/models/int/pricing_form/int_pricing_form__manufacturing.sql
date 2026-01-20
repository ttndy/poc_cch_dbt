-- update from last statement: 
-- 1. Add merge condition on scenario_year
-- 2. Add "MATERIAL OVERHEAD COST" AND "TRANSPO COST" account/driver 
SELECT 
     UPLOAD."material"                                      AS "material"
    ,UPLOAD."material_description"                          AS "material_description"
    ,UPLOAD."submisison_type"                               AS "submisison_type"
    ,UPLOAD."customer"                                      AS "customer"
    ,UPLOAD."department"                                    AS "department"
    ,UPLOAD."new_invoice"                                   AS "new_invoice"
    ,UPLOAD."new_retail"                                    AS "new_retail"
    ,UPLOAD."outdoor_standard_cost"                         AS "standard_cost"
    ,MFG."pricing_form_account"                             AS "pricing_form_account"
    ,ABS(MFG."value")                                       AS "rate"
    ,MFG."account"                                          AS "driver"
    ,MFG."scenario_year"                                    AS "rate_scenario"
    ,UPLOAD."material_overhead_cost"                        AS "mat_oh_cost"
FROM {{ ref('stg_pricing_form__pl_test') }} AS UPLOAD
    LEFT JOIN {{ ref('stg_pricing_form__manufacturing_rates')}} AS MFG
        ON UPLOAD."manufacturing_source"  = MFG."manufacturing_source"
        AND (
            UPPER(UPLOAD."shipping_terms") = UPPER(MFG."shipping_terms")
            OR  MFG."shipping_terms" = 'Blank'
        )
        AND UPLOAD."scenario_year" = MFG."scenario_year"

UNION ALL

SELECT 
     UPLOAD."material"                                  AS "material"
    ,UPLOAD."material_description"                      AS "material_description"
    ,UPLOAD."submisison_type"                           AS "submisison_type"
    ,UPLOAD."customer"                                  AS "customer"
    ,UPLOAD."department"                                AS "department"
    ,UPLOAD."new_invoice"                               AS "new_invoice"
    ,UPLOAD."new_retail"                                AS "new_retail"
    ,UPLOAD."outdoor_standard_cost"                     AS "standard_cost"
    ,'Material Overhead Cost'                           AS "pricing_form_account"
    ,ABS(UPLOAD."material_overhead_cost")               AS "rate"
    ,'Mat OH Cost'                                      AS "driver"
    ,UPLOAD."scenario_year"                             AS "rate_scenario"
    ,UPLOAD."material_overhead_cost"                    AS "mat_oh_cost"
FROM {{ ref('stg_pricing_form__pl_test') }} AS UPLOAD

UNION ALL

SELECT 
     UPLOAD."material"                                  AS "material"
    ,UPLOAD."material_description"                      AS "material_description"
    ,UPLOAD."submisison_type"                           AS "submisison_type"
    ,UPLOAD."customer"                                  AS "customer"
    ,UPLOAD."department"                                AS "department"
    ,UPLOAD."new_invoice"                               AS "new_invoice"
    ,UPLOAD."new_retail"                                AS "new_retail"
    ,UPLOAD."outdoor_standard_cost"                     AS "standard_cost"
    ,'Transport Rate'                                   AS "pricing_form_account"
    ,ABS(UPLOAD."transport_cost")                       AS "rate"
    ,'Transport Rate'                                   AS "driver"
    ,UPLOAD."scenario_year"                             AS "rate_scenario"
    ,UPLOAD."material_overhead_cost"                    AS "mat_oh_cost"
FROM {{ ref('stg_pricing_form__pl_test') }} AS UPLOAD