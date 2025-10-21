WITH BASE AS (
    SELECT 
         "material"                                                                     AS "material"
        ,"submisison_type"                                                              AS "submisison_type"
        ,"customer"                                                                     AS "customer"
        ,"department"                                                                   AS "department"
        ,"new_invoice"                                                                  AS "new_invoice"
        ,"driver"                                                                       AS "driver"
        ,"rate_scenario"                                                                AS "rate_scenario"
        ,ROW_NUMBER()
            OVER(
                PARTITION BY
                     "material"
                    ,"submisison_type"
                    ,"customer"
                    ,"department"
                    ,"new_invoice"
                    ,"driver"
                    ,"rate_scenario"
                ORDER BY   
                    "material" ASC
            )                                                                           AS "sequence"
        ,COALESCE(
            SUM(
                CASE 
                    WHEN "pricing_form_account" = 'Returns/DIF' THEN "rate"
                    ELSE 0
                END
            ) OVER(
                PARTITION BY
                     "material"
                    ,"submisison_type"
                    ,"customer"
                    ,"department"
                    ,"new_invoice"
                    ,"driver"
                    ,"rate_scenario"
            ),
            0
        )                                                                            AS "return_rate"
        ,COALESCE(
            SUM(
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
            ) OVER(
                PARTITION BY
                     "material"
                    ,"submisison_type"
                    ,"customer"
                    ,"department"
                    ,"new_invoice"
                    ,"driver"
                    ,"rate_scenario"
            ),
            0
        )                                                                           AS "sales_adj_rates"
        ,COALESCE(
            SUM(
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
            ) OVER(
                PARTITION BY
                     "material"
                    ,"submisison_type"
                    ,"customer"
                    ,"department"
                    ,"new_invoice"
                    ,"driver"
                    ,"rate_scenario"
            ),
            0
        )                                                                           AS "glr_adj_rates"
        ,CASE 
            WHEN "driver" = 'GLRCALCULATION$'
                THEN "new_invoice" - ("new_invoice" * "return_rate")
            WHEN "driver" IN ('SALES$', 'GROSSSALES$')
                THEN "new_invoice"
            WHEN "driver" = 'NETSALES$'
                THEN 
                    ("new_invoice" - ("new_invoice" * "return_rate"))
                    + (("new_invoice" - ("new_invoice" * "return_rate")) * "glr_adj_rates")
                    + ("new_invoice" * "sales_adj_rates")
            ELSE 
                "standard_cost"
        END                                                                         AS "value"
    FROM {{ref('fct_pricing_form__walk')}} 
    QUALIFY "sequence" = 1
)

SELECT 
     "material"                     AS "material"      
    ,"submisison_type"              AS "submisison_type"
    ,"customer"                     AS "customer"
    ,"department"                   AS "department"
    ,"new_invoice"                  AS "new_invoice" 
    ,"rate_scenario"                AS "rate_scenario" 
    ,"driver"                       AS "driver" 
    ,"value"                        AS "value" 
FROM BASE
