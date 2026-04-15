SELECT
     BSEG.BUKRS                               AS "company_code"
    ,BSEG.BELNR                               AS "document_number"
    ,BSEG.HKONT                               AS "gl_account" 
    ,BSEG.KOSTL                               AS "cost_center"
    ,BSEG.SGTXT                               AS "line_text"
    ,BSEG.GJAHR                               AS "year"
    ,BSEG.BUZEI                               AS "documnet_line"
    ,COALESCE(
        NULLIF(
            TRIM(BSEG.WERKS)
            ,''
        )
        ,'N/A'
    )                                         AS "plant"
    ,COALESCE(
        NULLIF(
            TRIM(BSEG.MATNR)
            ,''
        )
        ,'N/A'
    )                                         AS "material"
    ,COALESCE(
        NULLIF(
            TRIM(BSEG.ZZCUSTGRP)
            ,''
        )
        ,'N/A'
    )                                         AS "material_group"
    ,COALESCE(
        NULLIF(
            TRIM(BSEG.KUNNR)
            ,''
        )
        ,'N/A'
    )                                         AS "customer"
    ,COALESCE(
        NULLIF(
            TRIM(BSEG.ZZCUSTGRP)
            ,''
        )
        ,'N/A'
    )                                           AS "customer_group"
    ,CASE BSEG.SHKZG  
        WHEN 'H'
            THEN 'Credit'
        WHEN 'S'
            THEN 'Debit'
        END                                    AS "debit_credit"
    ,BSEG.AUGDT                                AS "clearing_date"
    ,BSEG.AUGCP                                AS "clearing_entry_date"
    ,BSEG.AUGBL                                AS "clearing_doc"
    ,BSEG.DMBTR                                AS "amount_in_local_currency"
    ,BSEG.WRBTR                                AS "amount"
    ,BSEG.KZBTR                                AS "origin_deduction_amount"
    ,BSEG.PSWBT                                AS "gl_amount" -- USE THIS?
    ,BSEG.PSWSL                                AS "gl_currency"
    ,BSEG.ZUONR                                AS "assignment" -- JOIN FOR WAR
    ,BSEG.PRCTR                                AS "profit_center"
    ,BSEG.PAOBJNR                              AS "profitability_segment"
    ,BSEG.SEGMENT                              AS "segment"
    ,BSEG.KSTAR                                AS "cost_element"
FROM {{source('sap_ecc', 'src_sap_ecc__bseg')}} AS BSEG 