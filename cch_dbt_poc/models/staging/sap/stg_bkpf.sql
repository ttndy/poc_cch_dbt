SELECT 
     BKPF.BUKRS          AS "company_code"
    ,BKPF.BELNR          AS "document_number"
    ,BKPF.GJAHR          AS "year"
    ,BKPF.MONAT          AS "period"
    ,BKPF.BLART          AS "document_type"
    ,BKPF.BUDAT          AS "posting_date"
    ,BKPF.BLDAT          AS "document_date"
    ,BKPF.WAERS          AS "transaction_currency"
    ,BKPF.HWAER          AS "currency_key"
    ,BKPF.USNAM          AS "posted_by"
    ,BKPF.BKTXT          AS "document_header_text"
    ,BKPF.XBLNR          AS "refrence"
FROM {{source('sap_ecc', 'src_sap_ecc__bkpf')}} AS BKPF 