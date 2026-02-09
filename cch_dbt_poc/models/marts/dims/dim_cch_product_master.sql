SELECT 
     MM."plant"                                            AS PLANT
    ,MM."material"                                         AS MODEL
    ,MM."material_description"                             AS MODEL_DESC
    ,MM."product_heairchy_level_1_code"                    AS PH_L1_CODE
    ,MM."product_heairchy_level_1"                         AS PH_L1_DESC
    ,MM."product_heairchy_level_2_code"                    AS PH_L2_CODE
    ,MM."product_heairchy_level_2"                         AS PH_L2_DESC
    ,MM."product_heairchy_level_3_code"                    AS PH_L3_CODE
    ,MM."product_heairchy_level_3"                         AS PH_L3_DESC
    ,MM."product_heairchy_level_4_code"                    AS PH_L4_CODE
    ,MM."product_heairchy_level_4"                         AS PH_L4_DESC
    ,MM."product_heairchy_level_5_code"                    AS PH_L5_CODE
    ,MM."product_heairchy_level_5"                         AS PH_L5_DESC
    ,MM."product_heairchy_level_6_code"                    AS PH_L6_CODE
    ,MM."product_heairchy_level_6"                         AS PH_L6_DESC
    ,MM."product_heairchy_level_7_code"                    AS PH_L7_CODE
    ,MM."product_heairchy_level_7"                         AS PH_L7_DESC
    ,MM."product_heairchy_level_8_code"                    AS PH_L8_CODE
    ,MM."product_heairchy_level_8"                         AS PH_L8_DESC
    ,MM."material_group_1_code"                            AS MG1_CODE
    ,MM."material_group_classification"                    AS MG1_DESC
    ,MM."material_group_2_code"                            AS MG2_CODE
    ,MM."kit_combination_classification"                   AS MG2_DESC
    ,MM."material_group_3_code"                            AS MG3_CODE
    ,MM."power_type_classification"                        AS MG3_DESC
    ,MM."material_group_4_code"                            AS MG4_CODE
    ,MM."manufacturing_classification"                     AS MG4_DESC
    ,MM."material_group_5_code"                            AS MG5_CODE
    ,MM."warranty_classification"                          AS MG5_DESC
    ,MM."material_external_group"                          AS EXTERNAL_MG_CODE
    ,MM."material_external_group_description"              AS EXTERNAL_MG_DESC
    ,MM."finacial_category_1_code"                         AS FINCAT1_COD
    ,MM."finacial_category_1"                              AS FINCAT1_DESC
    ,MM."finacial_category_2_code"                         AS FINCAT2_COD
    ,MM."finacial_category_2"                              AS FINCAT2_DESC
    ,MM."finacial_category_3_code"                         AS FINCAT3_COD
    ,MM."finacial_category_3"                              AS FINCAT3_DESC
    ,MM."finacial_category_4_code"                         AS FINCAT4_COD
    ,MM."finacial_category_4"                              AS FINCAT4_DESC             
    ,MM."finacial_category_5_code"                         AS FINCAT5_COD             
    ,MM."finacial_category_5"                              AS FINCAT5_DESC             
    ,COALESCE(MM."asia_category_code", 'Not Provided')     AS ASIACATCOD             
    ,COALESCE(MM."asia_category", 'Not Provided')          AS ASIACATDESC             
FROM {{ ref('dim_cch_material_master')}}  AS MM

UNION 

SELECT 
     'PLUG'                                                AS PLANT
    ,PARTS."material"                                      AS MODEL
    ,PARTS."material_description"                          AS MODEL_DESC
    ,PARTS."product_heairchy_level_1_code"                 AS PH_L1_CODE
    ,PARTS."product_heairchy_level_1"                      AS PH_L1_DESC
    ,PARTS."product_heairchy_level_2_code"                 AS PH_L2_CODE
    ,PARTS."product_heairchy_level_2"                      AS PH_L2_DESC
    ,PARTS."product_heairchy_level_3_code"                 AS PH_L3_CODE
    ,PARTS."product_heairchy_level_3"                      AS PH_L3_DESC
    ,PARTS."product_heairchy_level_4_code"                 AS PH_L4_CODE
    ,PARTS."product_heairchy_level_4"                      AS PH_L4_DESC
    ,PARTS."product_heairchy_level_5_code"                 AS PH_L5_CODE
    ,PARTS."product_heairchy_level_5"                      AS PH_L5_DESC 
    ,PARTS."product_heairchy_level_6_code"                 AS PH_L6_CODE
    ,PARTS."product_heairchy_level_6"                      AS PH_L6_DESC
    ,PARTS."product_heairchy_level_7_code"                 AS PH_L7_CODE
    ,PARTS."product_heairchy_level_7"                      AS PH_L7_DESC
    ,PARTS."product_heairchy_level_8_code"                 AS PH_L8_CODE
    ,PARTS."product_heairchy_level_8"                      AS PH_L8_DESC
    ,PARTS."management_group_1_code"                       AS MG1_CODE
    ,PARTS."management_group_1"                            AS MG1_DESC
    ,'0'                                                   AS MG2_CODE
    ,PARTS."kit_description"                               AS MG2_DESC
    ,'0'                                                   AS MG3_CODE
    ,PARTS."management_group_3"                            AS MG3_DESC
    ,PARTS."management_group_4_code"                       AS MG4_CODE
    ,PARTS."management_group_4"                            AS MG4_DESC
    ,'0'                                                   AS MG5_CODE
    ,'0'                                                   AS MG5_DESC
    ,PARTS."external_management_group_code"                AS EXTERNAL_MG_CODE
    ,PARTS."external_management_group"                     AS EXTERNAL_MG_DESC
    ,PARTS."finacial_category_1_code"                      AS FINCAT1_COD
    ,PARTS."finacial_category_1"                           AS FINCAT1_DESC
    ,PARTS."finacial_category_2_code"                      AS FINCAT2_COD
    ,PARTS."finacial_category_2"                           AS FINCAT2_DESC
    ,PARTS."finacial_category_3_code"                      AS FINCAT3_COD
    ,PARTS."finacial_category_3"                           AS FINCAT3_DESC
    ,PARTS."finacial_category_4_code"                      AS FINCAT4_COD  
    ,PARTS."finacial_category_4"                           AS FINCAT4_DESC  
    ,PARTS."finacial_category_5_code"                      AS FINCAT5_COD
    ,PARTS."finacial_category_5"                           AS FINCAT5_DESC
    ,PARTS."asia_category_code"                            AS ASIACATCOD
    ,PARTS."asia_category"                                 AS ASIACATDESC
FROM {{ ref('stg_cch_all_parts')}} AS PARTS
WHERE 
    PARTS."material" NOT IN (
        SELECT DISTINCT
            MARA."material"
        FROM {{ ref('stg_mara') }} AS MARA
    )
