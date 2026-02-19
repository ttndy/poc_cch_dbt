SELECT 
     COPA."scenario"                                                        AS "scenario"
    ,COPA."period"                                                          AS "period"
    ,COPA."period_raw"                                                      AS "period_raw"
    ,COPA."company_code"                                                    AS "company_code"
    ,CASE 
        WHEN COPA."company_code" != '1600' 
            THEN COPA."billing_type" 
        WHEN COPA."payer_number" = '0003010101'
            THEN 'BOPIS' 
        WHEN COPA."payer_number" = '0003010301'
            THEN 'SFS' 
        WHEN COPA."payer_number" = '0003010426' 
            THEN 'SFS-BULK'
        ELSE 
            COPA."billing_type" 
        END                                                                 AS "billing_type"
    ,COPA."sales_order_number"                                              AS "sales_order_number"
    ,VBAK."sales_order_code"                                                AS "sales_order_code"
    ,CASE
        WHEN COPA."company_code" = '1600'
            THEN COALESCE(STORE_MAP."payer_number", COPA."payer_number")
        WHEN PAYER_MM."customer_group_0" IN ('Intercompany', 'DTFO')
            THEN 'Intercompany'
        ELSE 
            COPA."payer_number" 
        END                                                                 AS "payer_number"
    ,COPA."material"                                                        AS "material"
    ,CASE
        WHEN COPA."billing_type" NOT IN (
            {{ var('return_types') }}
        )
            THEN COPA."quantity"
        ELSE 
            0
        END                                                                 AS "sales_quantity"
    ,CASE 
        WHEN (
            COPA."billing_type" NOT IN (
                {{ var('return_types') }}
            )
            AND COPA."billing_type" NOT IN (
                {{ var('man_sales_types') }}
            )
        )
           THEN COPA."sales_revenue"
        ELSE 
            0
        END                                                                 AS "gross_sales"
    ,CASE 
        WHEN COPA."billing_type" IN (
            {{ var('man_sales_types') }} 
        )
            THEN COPA."sales_revenue"
        ELSE 
            0
        END                                                                 AS "man_sales"
    ,CASE 
        WHEN COPA."billing_type" IN (
            {{ var('return_types') }}
        )
            THEN COPA."quantity"
        ELSE 
            0
        END                                                                 AS "returns_quantity"
    ,CASE 
        WHEN (
            COPA."billing_type" IN (
                 'RE'
                ,'ZS2'
            )
            AND 
            VBAK."sales_order_code" NOT IN (
                 '011'
                ,'013'
                ,'041'
                ,'051'
                ,'055'
                ,'065'
                ,'075'
                ,'116'
            )
        )
            THEN COPA."sales_revenue"
        ELSE 
            0
        END                                                             AS "sales_revenue_returns"
    ,CASE 
        WHEN (
            COPA."billing_type" IN (
                 'RE'
                ,'ZS2'
            )
            AND 
            VBAK."sales_order_code" IN (
                 '011'
                ,'013'
                ,'041'
                ,'051'
                ,'055'
                ,'065'
                ,'075'
                ,'116'
            )
        )
            THEN COPA."sales_revenue"
        ELSE 
            0
        END                                                             AS "buybacks"
    ,COPA."sales_revenue"                                               AS "sales_revenue"
    ,CASE 
        WHEN COPA."company_code" NOT IN (
            '1950'
        )
            THEN COPA."sales_deductions"
        WHEN "billing_type" NOT IN ('RE', 'ZM4')
            THEN COPA."sales_allowances"
        ELSE 
            0
        END * -1                                                        AS "sales_deductions"    -- Expense
    ,CASE  
        WHEN COPA."company_code" NOT IN (
            '1950'
        )
            THEN COPA."defective_allowance"
        ELSE 
             "sales_deductions" + COPA."defective_allowance"
        END * -1                                                         AS "defective_allowance" -- Expens
    ,CASE 
        WHEN COPA."billing_type" IN (
            {{ var('mat_cogs_types') }} 
        )
            THEN COPA."material_cogs"
        WHEN (
            COPA."billing_type" NOT IN (
                {{ var('return_types') }}
            )
            AND COPA."billing_type" NOT IN (
                {{ var('man_sales_types') }}
            )
        )
            THEN COPA."total_cogs" 
        ELSE
            0
        END * -1                                                         AS "standard_cogs" -- Expense
    ,CASE
        WHEN COPA."billing_type" IN (
            {{ var('man_sales_types') }}
        )
            THEN COPA."total_cogs" 
        ELSE 
            0
        END  * -1                                                       AS "man_standard_cogs" -- Expense
    ,CASE  
        WHEN COPA."billing_type" IN ('RE', 'ZS2')
            THEN COPA."total_cogs" 
        ELSE 
            0
        END * -1                                                        AS "return_cogs" -- Expense
    ,CASE  
        WHEN COPA."billing_type" IN ('ZM4')
            THEN COPA."total_cogs" 
        ELSE 
            0
        END  * -1                                                       AS "man_return_cogs" -- Expense
    ,CASE 
        WHEN (
            COPA."billing_type" NOT IN ('RE')
            AND COPA."company_code" != '1600'
        )
            THEN COPA."freight_expense"
        ELSE 
            0
        END                                                             AS "freight_out"
    ,CASE 
        WHEN (
            COPA."billing_type" IN ('RE')
            AND COPA."company_code" NOT IN ('1600')
        )
            THEN COPA."freight_expense"
        ELSE    
            0
        END                                                             AS "freigh_return"
    ,COPA."cash_discounts" * -1                                         AS "cash_discounts" -- Expense
    ,CASE
        WHEN (
            COPA."billing_type" NOT IN (
                 'RE'
                ,'ZM4'
            ) 
            AND COPA."company_code" NOT IN (
                '1950'
            )
        )
            THEN "sales_allowances"
        ELSE 
            0
        END  * -1                                                        AS "sales_allowances" -- Expense
    ,CASE  
        WHEN COPA."billing_type"  = 'ZM4'
            THEN COPA."sales_allowances"
                - COPA."sales_revenue"
        ELSE 
            0
        END * -1                                                        AS "sales_allowances_returns" -- Expense
    ,CASE
        WHEN COPA."billing_type" IN (
            'RE'
        )
            THEN COPA."sales_allowances"
        ELSE 
            0
        END * -1                                                        AS "sales_return_discr" -- Expense
    ,CASE
        WHEN COPA."billing_type" IN (
            'RE'
        )
            THEN COPA."sales_allowances"
        ELSE 
            0
        END * -1                                                       AS "warr_return_discr" -- Expense
    ,CASE 
        WHEN COPA."company_code" IN (
            '1950'
        )
            THEN COPA."market_fee"
        ELSE    
            0
        END * -1                                                       AS "market_fee" -- Expense
    ,CASE 
        WHEN COPA."company_code" IN (
            '1600'
        )
            THEN COPA."freight_expense"
        ELSE 
            0
        END                                                            AS "other_income" -- DTFO Freight Income
FROM {{ ref('stg_copa_4') }} AS COPA
    LEFT JOIN {{ ref('stg_vbak_refrence') }} AS VBAK
        ON COPA."sales_order_number" = VBAK."sales_order_number"
    LEFT JOIN {{ ref('stg_cch_payer_master')}}  AS PAYER_MM
        ON COPA."payer_number" = PAYER_MM."payer_number"
    LEFT JOIN {{ ref('stg_cch_dtfo_store_map')}} AS STORE_MAP
        ON COPA."customer" = STORE_MAP."customer"
        AND COPA."company_code" = '1600'
WHERE   
    COPA."WRTTP" = '10'
    AND COPA."company_code" IN ({{ var('cch_valid_company') }})
    AND LEFT(COPA."period_raw", 4) >= '2021'

