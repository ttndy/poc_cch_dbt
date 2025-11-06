SELECT
     PH6.WWPL6                  AS "material_product_hierarchy__6"
    ,PH6.BEZEK                  AS "material_product_hierarchy_description__6"
FROM {{ source('sap_ecc', 'src_sap_ecc__T25B5') }} AS PH6
WHERE 
    PH6.SPRAS = 'E'
