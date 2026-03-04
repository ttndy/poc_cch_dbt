-- update from last statement: 
-- 1. Add merge condition on scenario_year
-- 2. Add "MATERIAL OVERHEAD COST" AND "TRANSPO COST" account/driver 
SELECT 
     UPLOAD."material"                                      AS "material"
    ,UPLOAD."material_description"                          AS "material_description"
    ,UPLOAD."submission_type"                               AS "submission_type"
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
    ,UPLOAD."submission_type"                           AS "submission_type"
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
     PL."material"                                  AS "material"
    ,PL."material_description"                      AS "material_description"
    ,PL."submission_type"                           AS "submission_type"
    ,PL."customer"                                  AS "customer"
    ,PL."department"                                AS "department"
    ,PL."new_invoice"                               AS "new_invoice"
    ,PL."new_retail"                                AS "new_retail"
    ,PL."outdoor_standard_cost"                     AS "standard_cost"
    ,'Transport Rate'                                   AS "pricing_form_account"
    ,(MFG."value"+ PL."additional_freight")/NULLIF(PL."container_quantity",0)                     AS "rate"
    ,'Transport Rate'                                   AS "driver"
    ,PL."scenario_year"                             AS "rate_scenario"
    ,PL."material_overhead_cost"                    AS "mat_oh_cost"
FROM POC_CCH_DBT.DEV.STG_PRICING_FORM__PL_TEST PL 
LEFT JOIN POC_CCH_DBT.DEV.STG_PRICING_FORM__MANUFACTURING_RATES MFG
ON PL."manufacturing_source" = MFG."manufacturing_source"
AND PL."scenario_year" = MFG."scenario_year"
AND (
            UPPER(PL."shipping_terms") = UPPER(MFG."shipping_terms")
            OR  MFG."shipping_terms" = 'Blank'
        )
AND MFG."account" = 'Freight_Rate' 
