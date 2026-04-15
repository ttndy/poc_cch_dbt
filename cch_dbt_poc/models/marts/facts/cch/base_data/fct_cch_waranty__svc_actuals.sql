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
    ,WAR."material"                                     AS "material"
    ,'svc_expense'                                      AS "metric"
    ,-1 * SUM(BSEG."amount")                            AS "metric_value"
FROM {{ ref('stg_bseg') }} AS BSEG
    INNER JOIN {{ ref('stg_pnwtyh') }} AS WAR
        ON BSEG."assignment" = WAR."claim_number"
    INNER JOIN {{ ref('stg_faglflexa') }} AS FLEXA
        ON BSEG."document_number" = FLEXA."document_number"
        AND BSEG."documnet_line" = FLEXA."documnet_line"
        AND BSEG."company_code" = FLEXA."company_code"
        AND BSEG."year" = FLEXA."fiscal_year"
WHERE 
    BSEG."gl_account" LIKE ANY (
         '%603400'
        ,'%603411'
    )
    AND BSEG."company_code" IN ({{ var('cch_valid_company') }})
    AND YEAR(FLEXA."posting_date") >= 2023
GROUP BY 
    ALL
    