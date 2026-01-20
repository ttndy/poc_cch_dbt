WITH WALK AS (
    SELECT 
         "material"                                               AS "material"
        ,"material_description"                                   AS "material_description"
        ,"submisison_type"                                        AS "submisison_type"
        ,"customer"                                               AS "customer"
        ,"department"                                             AS "department"
        ,"new_invoice"                                            AS "new_invoice"
        ,"new_retail"                                             AS "new_retail"
        ,"standard_cost"                                          AS "standard_cost"
        ,"driver"                                                 AS "driver"
        ,"rate_scenario"                                          AS "rate_scenario"
        ,"pricing_form_account"                                   AS "pricing_form_account"
        ,"rate"                                                   AS "rate"
    FROM {{ref('fct_pricing_form__walk')}}
)

,AGG_RATES AS (
    SELECT 
         "material"                                         AS "material"
        ,"submisison_type"                                  AS "submisison_type"
        ,"customer"                                         AS "customer"
        ,"department"                                       AS "department"
        ,"new_invoice"                                      AS "new_invoice"
        ,"rate_scenario"                                    AS "rate_scenario"
        ,SUM(
            CASE 
                WHEN "pricing_form_account" = 'Returns/DIF' 
                THEN "rate" 
                ELSE 0 
            END
        )                                                   AS "return_rate"
        ,SUM(
            CASE 
                WHEN "pricing_form_account" IN (
                     'Contract Rebate'
                    ,'FIXED COOP'
                    ,'Cash Discount'
                    ,'EDI/RDC/IBX'
                    ,'Other'
                )
                AND "driver" = 'SALES$'
                THEN "rate"
                ELSE 0
            END
        )                                                   AS "sales_adj_rates"
        ,SUM(
            CASE 
                WHEN "pricing_form_account" IN (
                     'Contract Rebate'
                    ,'FIXED COOP'
                    ,'Cash Discount'
                    ,'EDI/RDC/IBX'
                    ,'Other'
                )
                AND "driver" = 'GLRCALCULATION$'
                THEN "rate"
                ELSE 0
            END
        )                                                   AS "glr_adj_rates"
    FROM WALK
    GROUP BY 
         "material"
        ,"submisison_type"
        ,"customer"
        ,"department"
        ,"new_invoice"
        ,"rate_scenario"
    )
,
DRIVERS AS (
    SELECT 
         WALK."material"                                    AS "material"
        ,WALK."material_description"                        AS "material_description"
        ,WALK."submisison_type"                             AS "submisison_type"
        ,WALK."customer"                                    AS "customer"
        ,WALK."department"                                  AS "department"
        ,WALK."new_invoice"                                 AS "new_invoice"
        ,WALK."new_retail"                                  AS "new_retail"
        ,WALK."standard_cost"                               AS "standard_cost"
        ,WALK."driver"                                      AS "driver"
        ,WALK."rate_scenario"                               AS "rate_scenario"
        ,COALESCE(AGG_RATES."return_rate", 0)               AS "return_rate"
        ,COALESCE(AGG_RATES."sales_adj_rates", 0)           AS "sales_adj_rates"
        ,COALESCE(AGG_RATES."glr_adj_rates", 0)             AS "glr_adj_rates"
        ,CASE 
            WHEN WALK."driver" = 'GLRCALCULATION$' THEN 
                WALK."new_invoice" + (WALK."new_invoice" * COALESCE(AGG_RATES."return_rate", 0))
            WHEN WALK."driver" IN ('SALES$', 'GROSSSALES$', 'OCOGS') THEN 
                WALK."new_invoice"
            WHEN WALK."driver" = 'NETSALES$' THEN 
                (WALK."new_invoice" + (WALK."new_invoice" * COALESCE(AGG_RATES."return_rate", 0)))
                + ((WALK."new_invoice" + (WALK."new_invoice" * COALESCE(AGG_RATES."return_rate", 0))) * COALESCE(AGG_RATES."glr_adj_rates", 0))
                + (WALK."new_invoice" * COALESCE(AGG_RATES."sales_adj_rates", 0))
            WHEN WALK."driver" = 'Mat OH Cost' THEN 
                WALK."rate"
            WHEN WALK."driver" = 'Markup %' THEN
                WALK."rate" * WALK."mat_oh_cost"
            ELSE WALK."standard_cost"
                
        END                                                 AS "value"
    FROM WALK
        LEFT JOIN AGG_RATES
            ON  WALK."material"        = AGG_RATES."material"
            AND WALK."submisison_type" = AGG_RATES."submisison_type"
            AND WALK."customer"        = AGG_RATES."customer"
            AND WALK."department"      = AGG_RATES."department"
            AND WALK."new_invoice"     = AGG_RATES."new_invoice"
            AND WALK."rate_scenario"   = AGG_RATES."rate_scenario"
)
SELECT 
     "material"
    ,"material_description"
    ,"submisison_type"
    ,"customer"
    ,"department"
    ,"new_invoice"
    ,"new_retail"
    ,"standard_cost"
    ,"driver"
    ,"rate_scenario"
    ,"return_rate"
    ,"sales_adj_rates"
    ,"glr_adj_rates"
    ,"value"
FROM DRIVERS
QUALIFY 
    ROW_NUMBER()
        OVER(
            PARTITION BY
                 "material"
                ,"submisison_type"
                ,"customer"
                ,"department"
                ,"new_invoice"
                ,"rate_scenario"
                ,"driver"
            ORDER BY  
                "material"
        ) = 1