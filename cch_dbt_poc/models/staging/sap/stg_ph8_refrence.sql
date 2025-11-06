SELECT
     PH8.WWPL8                  AS "material_product_hierarchy__8"
    ,PH8.BEZEK                  AS "material_product_hierarchy_description__8"
FROM {{ source('sap_ecc', 'src_sap_ecc__T25B8') }} AS PH8
WHERE 
    PH8.SPRAS = 'E'
