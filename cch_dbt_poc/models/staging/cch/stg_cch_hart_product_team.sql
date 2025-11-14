SELECT 
     CAST(HART_PM.PRODUCT AS VARCHAR)        AS "material"
    ,HART_PM.DIRECTOR                        AS "product_team"
    ,HART_PM.PM                              AS "sub_product_team"
FROM {{ source('raw_cch', 'src_cch__dim__hart_product_team')}} AS HART_PM
