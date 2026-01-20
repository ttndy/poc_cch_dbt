-- update from last statement: 
-- 1. Add merge condition on scenario_year
SELECT 
     UPLOAD."material"                                  AS "material"
    ,UPLOAD."material_description"                      AS "material_description"
    ,UPLOAD."submisison_type"                           AS "submisison_type"
    ,UPLOAD."customer"                                  AS "customer"
    ,UPLOAD."department"                                AS "department"
    ,UPLOAD."new_invoice"                               AS "new_invoice"
    ,UPLOAD."new_retail"                                AS "new_retail"
    ,UPLOAD."outdoor_standard_cost"                     AS "standard_cost"
    ,SGA."pricing_form_account"                         AS "pricing_form_account"
    ,ABS(SGA."value")                                   AS "rate"
    ,SGA."driver"                                       AS "driver"
    ,SGA."scenario_year"                                AS "rate_scenario"
    ,UPLOAD."material_overhead_cost"                    AS "mat_oh_cost"
FROM {{ ref('stg_pricing_form__pl_test') }} AS UPLOAD
    LEFT JOIN {{ ref('stg_pricing_form__sga_rates')}} AS SGA
        ON UPLOAD."material_type" = SGA."material_type"
        AND UPLOAD."form_of_power" = SGA."form_of_power"
        AND UPLOAD."category_group" = SGA."category_group"
        AND UPLOAD."scenario_year" = SGA."scenario_year"