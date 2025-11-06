SELECT
     PH3.WWPL3                  AS "material_product_hierarchy__3"
    ,PH3.BEZEK                  AS "material_product_hierarchy_description__3"
FROM {{ source('sap_ecc', 'src_sap_ecc__T25A9') }} AS PH3
WHERE 
    PH3.SPRAS = 'E'
