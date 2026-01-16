WITH BASE AS (
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
         "material"                                               AS "material"
        ,"submisison_type"                                        AS "submisison_type"
        ,"customer"                                               AS "customer"
        ,"department"                                             AS "department"
        ,"new_invoice"                                            AS "new_invoice"
        ,"rate_scenario"                                          AS "rate_scenario"
        ,SUM(
            CASE 
                WHEN "pricing_form_account" = 'Returns/DIF' 
                THEN "rate" 
                ELSE 0 
            END
        )                                                         AS "return_rate"
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
        )                                                         AS "sales_adj_rates"
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
        )                                                         AS "glr_adj_rates"
    FROM BASE
    GROUP BY 
         "material"
        ,"submisison_type"
        ,"customer"
        ,"department"
        ,"new_invoice"
        ,"rate_scenario"
)

,ENDING AS (
    SELECT 
         SRC."material"                                           AS "material"
        ,SRC."material_description"                               AS "material_description"
        ,SRC."submisison_type"                                    AS "submisison_type"
        ,SRC."customer"                                           AS "customer"
        ,SRC."department"                                         AS "department"
        ,SRC."new_invoice"                                        AS "new_invoice"
        ,SRC."new_retail"                                         AS "new_retail"
        ,SRC."standard_cost"                                      AS "standard_cost"
        ,SRC."driver"                                             AS "driver"
        ,SRC."rate_scenario"                                      AS "rate_scenario"
        ,COALESCE(RATES."return_rate", 0)                         AS "return_rate"
        ,COALESCE(RATES."sales_adj_rates", 0)                     AS "sales_adj_rates"
        ,COALESCE(RATES."glr_adj_rates", 0)                       AS "glr_adj_rates"
        ,CASE 
            WHEN SRC."driver" = 'GLRCALCULATION$' THEN 
                SRC."new_invoice" - (SRC."new_invoice" * COALESCE(RATES."return_rate", 0))
            WHEN SRC."driver" IN ('SALES$', 'GROSSSALES$') THEN 
                SRC."new_invoice"
            WHEN SRC."driver" = 'NETSALES$' THEN 
                (SRC."new_invoice" - (SRC."new_invoice" * COALESCE(RATES."return_rate", 0)))
                + ((SRC."new_invoice" - (SRC."new_invoice" * COALESCE(RATES."return_rate", 0))) * COALESCE(RATES."glr_adj_rates", 0))
                + (SRC."new_invoice" * COALESCE(RATES."sales_adj_rates", 0))
            ELSE SRC."standard_cost"
        END                                                      AS "value"
    FROM BASE AS SRC
    LEFT JOIN AGG_RATES AS RATES
        ON  SRC."material"        = RATES."material"
        AND SRC."submisison_type" = RATES."submisison_type"
        AND SRC."customer"        = RATES."customer"
        AND SRC."department"      = RATES."department"
        AND SRC."new_invoice"     = RATES."new_invoice"
        AND SRC."rate_scenario"   = RATES."rate_scenario"
)

SELECT 
     ENDING."material"                                           AS "material"
    ,ENDING."material_description"                               AS "material_description"
    ,ENDING."submisison_type"                                    AS "submisison_type"
    ,ENDING."customer"                                           AS "customer"
    ,ENDING."department"                                         AS "department"
    ,ENDING."new_invoice"                                        AS "new_invoice"
    ,ENDING."new_retail"                                         AS "new_retail"
    ,ENDING."standard_cost"                                      AS "standard_cost"
    ,ENDING."driver"                                             AS "driver"
    ,ENDING."rate_scenario"                                      AS "rate_scenario"
    ,ENDING."return_rate"                                        AS "return_rate"
    ,ENDING."sales_adj_rates"                                    AS "sales_adj_rates"
    ,ENDING."glr_adj_rates"                                      AS "glr_adj_rates"
    ,ENDING."value"                                              AS "value"
FROM ENDING
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
