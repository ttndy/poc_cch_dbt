SELECT
     COPA."scenario"                                                            AS "scenario"
    ,COPA."period"                                                              AS "period"
    ,COPA."period_raw"                                                          AS "period_raw"
    ,COPA."company_code"                                                        AS "company_code"
    /* normalized billing type */
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
     END                                                                        AS "billing_type_adj"
    ,COPA."sales_order_number"                                                  AS "sales_order_number" 
    ,VBAK."sales_order_code"                                                    AS "sales_order_code"
    /* normalized payer */
    ,CASE
        WHEN COPA."company_code" = '1600'
            THEN COALESCE(STORE_MAP."payer_number", COPA."payer_number")
        WHEN PAYER_MM."customer_group_0" IN ('Intercompany','DTFO')
            THEN 'Intercompany'
        ELSE COPA."payer_number"
     END                                                                        AS "payer_number_adj"
    /* raw facts */
    ,COPA."material"                                                            AS "material"
    ,COPA."quantity"                                                            AS "quantity"
    ,COPA."sales_revenue"                                                       AS "sales_revenue"
    ,COPA."sales_deductions"                                                    AS "sales_deductions"
    ,COPA."defective_allowance"                                                 AS "defective_allowance"
    ,COPA."sales_allowances"                                                    AS "sales_allowances"
    ,COPA."material_cogs"                                                       AS "material_cogs"
    ,COPA."total_cogs"                                                          AS "total_cogs"
    ,COPA."freight_expense"                                                     AS "freight_expense"
    ,COPA."cash_discounts"                                                      AS "cash_discounts"
    ,COPA."market_fee"                                                          AS "market_fee"
    /* ===== CORE FLAGS ===== */
    ,CASE
        WHEN COPA."billing_type" = 'ZM4'
            THEN 'man_returns'
        WHEN COPA."billing_type" IN ({{ var('return_types') }}) 
            THEN 'returns'
        WHEN COPA."billing_type" IN ({{ var('man_sales_types') }}) 
            THEN 'man_sales'
        WHEN COPA."billing_type" IN ({{ var('mat_cogs_types') }}) 
            THEN 'mat_cogs'
        ELSE 
            'normal'
     END                                                                        AS "billing_group"
FROM {{ ref('stg_copa_4') }} COPA
    LEFT JOIN {{ ref('stg_vbak_refrence') }} VBAK
        ON COPA."sales_order_number" = VBAK."sales_order_number"
    LEFT JOIN {{ ref('stg_cch_payer_master')}} PAYER_MM
        ON COPA."payer_number" = PAYER_MM."payer_number"
    LEFT JOIN {{ ref('stg_cch_dtfo_store_map')}} STORE_MAP
        ON COPA."customer" = STORE_MAP."customer"
        AND COPA."company_code" = '1600'
WHERE
    COPA."WRTTP" = '10'
    AND COPA."company_code" IN ({{ var('cch_valid_company') }})
    AND LEFT(COPA."period_raw", 4) >= '2021'
