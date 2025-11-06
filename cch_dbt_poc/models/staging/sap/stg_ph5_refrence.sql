SELECT
     PH5.WWPL5                  AS "material_product_hierarchy__5"
    ,PH5.BEZEK                  AS "material_product_hierarchy_description__5"
FROM {{ source('sap_ecc', 'src_sap_ecc__T25B1') }} AS PH5
WHERE 
    PH5.SPRAS = 'E'
