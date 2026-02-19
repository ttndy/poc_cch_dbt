SELECT
     COPA."scenario"                                                  AS "scenario"
    ,COPA."period"                                                    AS "period"
    ,COPA."period_raw"                                                AS "period_raw"
    ,COPA."company_code"                                              AS "company_code"
    ,COPA."billing_type_adj"                                          AS "billing_type"                                      
    ,COPA."sales_order_number"                                        AS "sales_order_number"
    ,COPA."sales_order_code"                                          AS "sales_order_code"
    ,COPA."payer_number_adj"                                          AS "payer_number"                                         
    ,COPA."material"                                                  AS "material"
    ,METRIC_RULES.metric_name                                         AS "metric"
    ,CASE METRIC_RULES.value_field
        WHEN 'quantity' 
            THEN COPA."quantity"
        WHEN 'sales_revenue' 
            THEN COPA."sales_revenue"
        WHEN 'total_cogs' 
            THEN COPA."total_cogs"
        WHEN 'material_cogs' 
            THEN COPA."material_cogs"
        WHEN 'sales_allowances' 
            THEN COPA."sales_allowances"
        WHEN 'sales_deductions' 
            THEN COPA."sales_deductions"
        WHEN 'sales_allow_and_defective' 
            THEN COPA."defective_allowance" 
                + COPA."sales_allowances"
        WHEN 'defective_allowance' 
            THEN COPA."defective_allowance"
        WHEN 'sales_allow_less_rev'
            THEN COPA."sales_allowances"
                - COPA."sales_revenue"
        WHEN 'freight_expense' 
            THEN COPA."freight_expense"
        WHEN 'cash_discounts' 
            THEN COPA."cash_discounts"
        WHEN 'market_fee' 
            THEN COPA."market_fee"
        ELSE 0
     END 
     * METRIC_RULES.sign                                             AS "metric_value"
FROM {{ ref('int_copa_base') }} AS COPA 
    CROSS JOIN {{ ref('copa_metrics_rules') }} AS METRIC_RULES
WHERE 
    (
        METRIC_RULES.company_code_filter IS NULL
        OR (
            METRIC_RULES.company_code_filter LIKE 'NOT:%'
            AND NOT ARRAY_CONTAINS(
                    SPLIT(TRIM(METRIC_RULES.company_code_filter), '|'), 
                    COPA."company_code"::VARIANT
                )
        )
        OR (
            METRIC_RULES.company_code_filter LIKE 'IN:%'
            AND ARRAY_CONTAINS(
                SPLIT(TRIM(METRIC_RULES.company_code_filter), '|'), 
                COPA."company_code"::VARIANT
            )
        )
    )
    AND (
        METRIC_RULES.biling_group_filter IS NULL
        OR (
            METRIC_RULES.biling_group_filter LIKE 'NOT:%'
            AND NOT ARRAY_CONTAINS(
                    SPLIT(TRIM(METRIC_RULES.biling_group_filter), '|'), 
                    COPA."billing_group"::VARIANT
                )
        )
        OR (
            METRIC_RULES.biling_group_filter LIKE 'IN:%'
            AND ARRAY_CONTAINS(
                SPLIT(TRIM(METRIC_RULES.biling_group_filter), '|'), 
                COPA."billing_group"::VARIANT
            )
        )
    )
    AND (
        METRIC_RULES.sales_order_code_filter IS NULL
        OR (
            METRIC_RULES.sales_order_code_filter LIKE 'NOT:%'
            AND NOT ARRAY_CONTAINS(
                    SPLIT(TRIM(METRIC_RULES.sales_order_code_filter), '|'), 
                    COPA."sales_order_code"::VARIANT
                )
        )
        OR (
            METRIC_RULES.sales_order_code_filter LIKE 'IN:%'
            AND ARRAY_CONTAINS(
                SPLIT(TRIM(METRIC_RULES.sales_order_code_filter), '|'), 
                COPA."sales_order_code"::VARIANT
            )
        )
    )