SELECT DISTINCT
     VBAK.VBELN             AS "sales_order_number"
    ,VBAK.AUGRU             AS "sales_order_code"
FROM {{source('sap_ecc', 'src_sap_ecc__vbak')}} AS VBAK
