SELECT
     PH1.WWPL1                  AS "material_product_hierarchy__1"
    ,PH1.BEZEK                  AS "material_product_hierarchy_description__1"
FROM {{ source('sap_ecc', 'src_sap_ecc__T25A7') }} AS PH1
WHERE 
    PH1.SPRAS = 'E'
