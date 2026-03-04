-- update from last statement:
-- 1. Add merge condition on scenario_year=
SELECT 
         UPLOAD."material"                                  AS "material"
        ,UPLOAD."material_description"                      AS "material_description"
        ,UPLOAD."submission_type"                           AS "submission_type"
        ,UPLOAD."customer"                                  AS "customer"
        ,UPLOAD."department"                                AS "department"
        ,UPLOAD."new_invoice"                               AS "new_invoice"
        ,UPLOAD."new_retail"                                AS "new_retail"
        ,UPLOAD."outdoor_standard_cost"                     AS "standard_cost"
        ,BASE_DRIVER."pricing_form_account"                 AS "pricing_form_account"
        ,ABS(BASE_DRIVER."value")                           AS "rate"
        ,BASE_DRIVER."driver_account"                       AS "driver"
        ,BASE_DRIVER."scenario_year"                        AS "rate_scenario"
        ,UPLOAD."material_overhead_cost"                    AS "mat_oh_cost"
FROM {{ ref('stg_pricing_form__pl_test') }} AS UPLOAD
    LEFT JOIN {{ ref('stg_pricing_form__base_drivers')}} AS BASE_DRIVER
        ON UPLOAD."customer" = BASE_DRIVER."customer"
        AND UPLOAD."department" = BASE_DRIVER."department"
        AND UPLOAD."scenario_year" = BASE_DRIVER."scenario_year"