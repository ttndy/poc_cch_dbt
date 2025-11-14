SELECT 
     MARC."plant"                                                           AS "plant"
    ,MARA."material"                                                        AS "material"
    ,MAKT."material_description"                                            AS "material_description"
    ,MARA."material_product_hierarchy__1"                                   AS "product_heairchy_level_1_code"
    ,PH1."material_product_hierarchy_description__1"                        AS "product_heairchy_level_1"
    ,MARA."material_product_hierarchy__2"                                   AS "product_heairchy_level_2_code"
    ,PH2."material_product_hierarchy_description__2"                        AS "product_heairchy_level_2"
    ,MARA."material_product_hierarchy__3"                                   AS "product_heairchy_level_3_code"
    ,PH3."material_product_hierarchy_description__3"                        AS "product_heairchy_level_3"
    ,MARA."material_product_hierarchy__4"                                   AS "product_heairchy_level_4_code"
    ,PH4."material_product_hierarchy_description__4"                        AS "product_heairchy_level_4"
    ,MARA."material_product_hierarchy__5"                                   AS "product_heairchy_level_5_code"
    ,PH5."material_product_hierarchy_description__5"                        AS "product_heairchy_level_5"
    ,MARA."material_product_hierarchy__6"                                   AS "product_heairchy_level_6_code"
    ,PH6."material_product_hierarchy_description__6"                        AS "product_heairchy_level_6"
    ,MARA."material_product_hierarchy__7"                                   AS "product_heairchy_level_7_code"
    ,PH7."material_product_hierarchy_description__7"                        AS "product_heairchy_level_7"
    ,MARA."material_product_hierarchy__8"                                   AS "product_heairchy_level_8_code"
    ,PH8."material_product_hierarchy_description__8"                        AS "product_heairchy_level_8"
    ,MVKE."material_group_1_code"                                           AS "material_group_1_code"
    ,MVKE."material_group_classification"                                   AS "material_group_classification"
    ,MVKE."material_group_2_code"                                           AS "material_group_2_code"
    ,MVKE."kit_combination_classification"                                  AS "kit_combination_classification"
    ,MVKE."material_group_3_code"                                           AS "material_group_3_code"
    ,MVKE."power_type_classification"                                       AS "power_type_classification"
    ,MVKE."material_group_4_code"                                           AS "material_group_4_code"
    ,MVKE."manufacturing_classification"                                    AS "manufacturing_classification"
    ,MVKE."material_group_5_code"                                           AS "material_group_5_code"
    ,MVKE."warranty_classification"                                         AS "warranty_classification"
    ,MARA."material_external_group"                                         AS "material_external_group"
    ,TWEWT."material_external_group_description"                            AS "material_external_group_description"
    ,'0'                                                                    AS "finacial_category_1_code"
    ,FINCAT."fincat_1_description"                                          AS "finacial_category_1"
    ,'0'                                                                    AS "finacial_category_2_code"
    ,FINCAT."fincat_2_description"                                          AS "finacial_category_2"
    ,'0'                                                                    AS "finacial_category_3_code"
    ,CASE 
        WHEN MARA."material_product_hierarchy__2" = '3'
            THEN 'Parts'
        WHEN MARA."material_product_hierarchy__1" = '2'
            THEN  HART_PM."product_team"
        WHEN MARA."material_product_hierarchy__1" = '3'
            THEN PM."product_team"
        ELSE 
            PM."product_team"
        END                                                                 AS "finacial_category_3"
    ,'0'                                                                    AS "finacial_category_4_code"
    ,CASE 
        WHEN MARA."material_product_hierarchy__2" = '3'
            THEN 'Parts'
        WHEN MARA."material_product_hierarchy__1" = '2'
            THEN  HART_PM."sub_product_team"
        WHEN MARA."material_product_hierarchy__1" = '3'
            THEN PM."sub_product_team"
        ELSE 
            PM."sub_product_team"
        END                                                                 AS "finacial_category_4"
    ,'0'                                                                    AS "finacial_category_5_code"
    ,'0'                                                                    AS "finacial_category_5"
    ,ASIA."variance_alloaction_code"                                        AS "asia_category_code"
    ,ASIA."variance_alloaction"                                             AS "asia_category"
FROM {{ref('stg_mara')}} AS MARA
    LEFT JOIN {{ref('stg_marc')}} AS MARC
        ON MARA."material" = MARC."material"
    LEFT JOIN {{ref('stg_mvke')}} AS MVKE
        ON MARA."material" = MVKE."material"
        AND MARC."plant" = MVKE."plant"
    LEFT JOIN {{ref('stg_makt')}} AS MAKT
        ON MARA."material" = MAKT."material"
    LEFT JOIN {{ref('stg_ph1_refrence')}} AS PH1
        ON MARA."material_product_hierarchy__1" = PH1."material_product_hierarchy__1"
    LEFT JOIN {{ref('stg_ph2_refrence')}} AS PH2
        ON MARA."material_product_hierarchy__2" = PH2."material_product_hierarchy__2"
    LEFT JOIN {{ref('stg_ph3_refrence')}} AS PH3
        ON MARA."material_product_hierarchy__3" = PH3."material_product_hierarchy__3"
    LEFT JOIN {{ref('stg_ph4_refrence')}} AS PH4
        ON MARA."material_product_hierarchy__4" = PH4."material_product_hierarchy__4"
    LEFT JOIN {{ref('stg_ph5_refrence')}} AS PH5
        ON MARA."material_product_hierarchy__5" = PH5."material_product_hierarchy__5"
    LEFT JOIN {{ref('stg_ph6_refrence')}} AS PH6
        ON MARA."material_product_hierarchy__6" = PH6."material_product_hierarchy__6"
    LEFT JOIN {{ref('stg_ph7_refrence')}} AS PH7
        ON MARA."material_product_hierarchy__7" = PH7."material_product_hierarchy__7"
    LEFT JOIN {{ref('stg_ph8_refrence')}} AS PH8
        ON MARA."material_product_hierarchy__8" = PH8."material_product_hierarchy__8"
    LEFT JOIN {{ref('stg_twewt')}} AS TWEWT
        ON MARA."material_external_group" = TWEWT."material_external_group"
    LEFT JOIN {{ ref('stg_cch_fincats') }} AS FINCAT
        ON  {{ star_join('PH1."material_product_hierarchy_description__1"', 'FINCAT."material_product_hierarchy_description__1"') }}
        AND {{ star_join('PH2."material_product_hierarchy_description__2"', 'FINCAT."material_product_hierarchy_description__2"') }}
        AND {{ star_join('PH3."material_product_hierarchy_description__3"', 'FINCAT."material_product_hierarchy_description__3"') }}
        AND {{ star_join('PH4."material_product_hierarchy_description__4"', 'FINCAT."material_product_hierarchy_description__4"') }}
        AND {{ star_join('PH5."material_product_hierarchy_description__5"', 'FINCAT."material_product_hierarchy_description__5"') }}
        AND {{ star_join('PH6."material_product_hierarchy_description__6"', 'FINCAT."material_product_hierarchy_description__6"') }}
        AND {{ star_join('PH7."material_product_hierarchy_description__7"', 'FINCAT."material_product_hierarchy_description__7"') }}
        AND {{ star_join('PH8."material_product_hierarchy_description__8"', 'FINCAT."material_product_hierarchy_description__8"') }}
    LEFT JOIN {{ ref('stg_cch_product_team')}} AS PM
        ON  {{ star_join('PH1."material_product_hierarchy_description__1"', 'PM."material_product_hierarchy_description__1"') }}
        AND {{ star_join('PH2."material_product_hierarchy_description__2"', 'PM."material_product_hierarchy_description__2"') }}
        AND {{ star_join('PH3."material_product_hierarchy_description__3"', 'PM."material_product_hierarchy_description__3"') }}
        AND {{ star_join('PH4."material_product_hierarchy_description__4"', 'PM."material_product_hierarchy_description__4"') }}
        AND {{ star_join('PH5."material_product_hierarchy_description__5"', 'PM."material_product_hierarchy_description__5"') }}
        AND {{ star_join('PH6."material_product_hierarchy_description__6"', 'PM."material_product_hierarchy_description__6"') }}
        AND {{ star_join('PH7."material_product_hierarchy_description__7"', 'PM."material_product_hierarchy_description__7"') }}
        AND {{ star_join('PH8."material_product_hierarchy_description__8"', 'PM."material_product_hierarchy_description__8"') }}
    LEFT JOIN {{ ref('stg_cch_hart_product_team')}} AS HART_PM
        ON TRIM(MARA."material") = TRIM(HART_PM."material")
    LEFT JOIN {{ ref('stg_cch_asia_classifactions')}} AS ASIA
        ON  MARA."material_product_hierarchy__1" = ASIA."material_product_hierarchy__1"
        AND MARA."material_product_hierarchy__2" = ASIA."material_product_hierarchy__2"
        AND MARA."material_product_hierarchy__4" = ASIA."material_product_hierarchy__4"
        AND MARA."material_product_hierarchy__6" = ASIA."material_product_hierarchy__6"
        AND MARA."material_product_hierarchy__7" = ASIA."material_product_hierarchy__7"
        AND MARA."material_product_hierarchy__8" = ASIA."material_product_hierarchy__8"
        AND MVKE."material_group_4_code"  = ASIA."material_group_4_code"
