WITH BASE AS (
    SELECT  
         COPA."scenario"                   AS "scenario"
        ,COPA."period"                     AS "period"
        ,COPA."company_code"               AS "company_code"
        ,COPA."payer_number"               AS "payer_number"
        ,SUM("metric_value")               AS "value"
    FROM {{ ref('fct_cch_copa_metrics') }} AS COPA
    WHERE 
        COPA."metric" = 'sales_revenue'
        AND COPA."metric_value" != 0 
    GROUP BY 
         COPA."scenario"  
        ,COPA."period"       
        ,COPA."company_code"    
        ,COPA."payer_number" 
)

SELECT
     "scenario"                            AS "scenario"     
    ,"period"                              AS "period"     
    ,"company_code"                        AS "company_code"
    ,"payer_number"                        AS "payer_number"
    ,"value" /
        SUM("value")
            OVER(
                PARTITION BY
                     "scenario"    
                    ,"period"      
                    ,"company_code" 
            )                               AS "driver"
FROM BASE 
QUALIFY 
    SUM("value")
        OVER(
            PARTITION BY
                 "scenario"    
                ,"period"      
                ,"company_code"  
        )   != 0 
    