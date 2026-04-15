SELECT 
     PNWTYH.AU_CLMNO             AS "claim_number"
    ,PNWTYH.AU_CLMTY             AS "claim_type"
    ,PNWTYH.AU_RLOBEX            AS "material"
FROM {{source('sap_ecc', 'src_sap_ecc__pnwtyh')}} AS PNWTYH
    