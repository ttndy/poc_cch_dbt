SELECT 
     FINCAT.PH_1_DESC        AS "material_product_hierarchy_description__1"
    ,FINCAT.PH_2_DESC        AS "material_product_hierarchy_description__2"
    ,FINCAT.PH_3_DESC        AS "material_product_hierarchy_description__3"
    ,FINCAT.PH_4_DESC        AS "material_product_hierarchy_description__4"
    ,FINCAT.PH_5_DESC        AS "material_product_hierarchy_description__5"
    ,FINCAT.PH_6_DESC        AS "material_product_hierarchy_description__6"
    ,FINCAT.PH_7_DESC        AS "material_product_hierarchy_description__7"
    ,FINCAT.PH_8_DESC        AS "material_product_hierarchy_description__8"
    ,FINCAT.FINCAT1_DESC     AS "fincat_1_description"
    ,FINCAT.FINCAT2_DESC     AS "fincat_2_description"
FROM {{source('raw_cch', 'src_cch__fin_cats')}} AS FINCAT
