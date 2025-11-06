SELECT
     PH7.WWPL6                  AS "material_product_hierarchy__7"
    ,PH7.BEZEK                  AS "material_product_hierarchy_description__7"
FROM {{ source('sap_ecc', 'src_sap_ecc__T25B6') }} AS PH7
WHERE 
    PH7.SPRAS = 'E'
