SELECT 
     MAKT.MATNR                                 AS "material"
    ,MAKT.MAKTX                                 AS "material_description"
FROM {{source('sap_ecc', 'src_sap_ecc__makt')}} AS MAKT
WHERE  
    MAKT.SPRAS = 'E'
