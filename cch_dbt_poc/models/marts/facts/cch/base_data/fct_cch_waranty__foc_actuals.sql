SELECT 
     YEAR(FLEXA."posting_date") 
        || 'ACT'                                        AS "scenario"
    ,LPAD(
        MONTH(FLEXA."posting_date")
        ,2
        ,'0'
    )                                                   AS "period"
    ,'0' 
        || LPAD(
            MONTH(FLEXA."posting_date")
            ,2
            ,'0'
        )   
        || '/'
        || YEAR(FLEXA."posting_date")                   AS "period_raw"
    ,BSEG."company_code"                                AS "company_code"
    ,'F2'                                               AS "billing_type"
    ,'N/A'                                              AS "sales_order_number"
    ,'N/A'                                              AS "sales_order_code"
    ,BSEG."customer"                                    AS "payer_number"  -- Need to do payer assignment from COPA
    ,BSEG."material"                                    AS "material"
    ,'svc_expense'                                      AS "metric"
    ,-1 * SUM(BSEG."amount")                            AS "metric_value"
FROM {{ ref('stg_faglflexa') }} AS FLEXA
    LEFT JOIN {{ ref('stg_bseg') }} AS BSEG
        ON  FLEXA."document_number" = BSEG."document_number"
        AND FLEXA."documnet_line" = BSEG."documnet_line"
        AND FLEXA."company_code" = BSEG."company_code"
        AND FLEXA."fiscal_year" = BSEG."year" 
WHERE 
    FLEXA."gl_account" LIKE '%709900'
    AND FLEXA."cost_center" IN (
         '0014080030'
        ,'0013080030'
        ,'0093080030'
        ,'0094080030'
    )
    AND FLEXA."company_code" IN ({{ var('cch_valid_company') }})
    AND FLEXA."year" >= 2023
    