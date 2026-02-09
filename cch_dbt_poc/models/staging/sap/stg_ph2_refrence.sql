SELECT
     PH2.WWPL2                  AS "material_product_hierarchy__2"
    ,PH2.BEZEK                  AS "material_product_hierarchy_description__2"
FROM {{ source('sap_ecc', 'src_sap_ecc__T25A8') }} AS PH2
WHERE 
    PH2.SPRAS = 'E'
