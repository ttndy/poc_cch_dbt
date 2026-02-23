SELECT 
     metric_name                                    AS metric_name
    ,value_field                                    AS value_field
    ,sign                                           AS sign
    ,company_code_filter LIKE 'IN%'                 AS include_company_code_list
    ,SPLIT(
        REGEXP_REPLACE(
            company_code_filter
            ,'^(IN:|NOT:)'
            ,''
        )
        ,'|'
    )                                               AS company_code_list
    ,biling_group_filter LIKE 'IN%'                 AS include_billing_group_list
    ,SPLIT(
        REGEXP_REPLACE(
            biling_group_filter
            ,'^(IN:|NOT:)'
            ,''
        )
        ,'|'
    )                                               AS billing_group_list
    ,sales_order_code_filter LIKE 'IN%'             AS include_sales_order_code_list
    ,SPLIT(
        REGEXP_REPLACE(
            sales_order_code_filter
            ,'^(IN:|NOT:)'
            ,''
        )
        ,'|'
    )                                               AS sales_order_code_list
FROM {{ ref('copa_metrics_rules') }}