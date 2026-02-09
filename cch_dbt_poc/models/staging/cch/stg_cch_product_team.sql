SELECT 
     PM.PH_1_DESC               AS "material_product_hierarchy_description__1"
    ,PM.PH_2_DESC               AS "material_product_hierarchy_description__2"
    ,PM.PH_3_DESC               AS "material_product_hierarchy_description__3"
    ,PM.PH_4_DESC               AS "material_product_hierarchy_description__4"
    ,PM.PH_5_DESC               AS "material_product_hierarchy_description__5"
    ,PM.PH_6_DESC               AS "material_product_hierarchy_description__6"
    ,PM.PH_7_DESC               AS "material_product_hierarchy_description__7"
    ,PM.PH_8_DESC               AS "material_product_hierarchy_description__8"
    ,PM.PRODUCT_TEAM            AS "product_team"
    ,PM.SUB_PRODUCT_TEAM        AS "sub_product_team"
FROM {{ source('raw_cch', 'src_cch__dim_product_team')}} AS PM
