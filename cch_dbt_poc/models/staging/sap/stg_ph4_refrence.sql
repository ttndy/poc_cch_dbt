SELECT
     PH4.WWPL4                  AS "material_product_hierarchy__4"
    ,PH4.BEZEK                  AS "material_product_hierarchy_description__4"
FROM {{ source('sap_ecc', 'src_sap_ecc__T25B0') }} AS PH4
WHERE 
    PH4.SPRAS = 'E'
