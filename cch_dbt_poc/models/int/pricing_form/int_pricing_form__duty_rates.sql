-- new addition on feb 03 2026 
SELECT 
     UPLOAD."material"                                  AS "material"
    ,UPLOAD."material_description"                      AS "material_description"
    ,UPLOAD."submission_type"                          AS "submission_type"
    ,UPLOAD."customer"                                  AS "customer"
    ,UPLOAD."department"                                AS "department"
    ,UPLOAD."new_invoice"                               AS "new_invoice"
    ,UPLOAD."new_retail"                                AS "new_retail"
    ,UPLOAD."outdoor_standard_cost"                     AS "standard_cost"
    ,'Duty'                                             AS "pricing_form_account"
    ,CASE 
        WHEN UPLOAD."customer" LIKE ('%Direct Import%') THEN 0 
        ELSE DUTY."value"                                      
        END                                             AS "rate"
    ,'DUTY$'                                            AS "driver"
    ,UPLOAD."scenario_year"                             AS "rate_scenario"
    ,UPLOAD."material_overhead_cost"                    AS "mat_oh_cost"
FROM {{ ref('stg_pricing_form__pl_test') }} AS UPLOAD
    LEFT JOIN {{ ref('stg_pricing_form__duty_rates')}} AS DUTY
        WHERE UPLOAD."category_group" = DUTY."category_group"
        AND   UPLOAD."scenario_year" = DUTY."scenario_year"


    