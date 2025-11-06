SELECT 
	 MVKE.MATNR  		AS "material"
    ,MVKE.DWERK			AS "plant"
	,MVKE.VKORG  		AS "sales_org"
	,MVKE.MVGR1 		AS "material_group_1_code"
    ,TVM1T.BEZEI        AS "material_group_classification"
	,MVKE.MVGR2 		AS "material_group_2_code"
    ,TVM2T.BEZEI        AS "kit_combination_classification"
	,MVKE.MVGR3 		AS "material_group_3_code"
    ,TVM3T.BEZEI        AS "power_type_classification"
	,MVKE.MVGR4 		AS "material_group_4_code"
    ,TVM4T.BEZEI        AS "manufacturing_classification"
	,MVKE.MVGR5 		AS "material_group_5_code"
    ,TVM5T.BEZEI        AS "warranty_classification"
FROM {{source('sap_ecc', 'src_sap_ecc__mvke')}} AS MVKE
    LEFT JOIN {{source('sap_ecc', 'src_sap_ecc__tvm1t')}} AS TVM1T
        ON MVKE.MVGR1 = TVM1T.MVGR1
        AND TVM1T.SPRAS = 'E'
    LEFT JOIN {{source('sap_ecc', 'src_sap_ecc__tvm2t')}} AS TVM2T
        ON MVKE.MVGR2 = TVM2T.MVGR2
        AND TVM1T.SPRAS = 'E'
    LEFT JOIN {{source('sap_ecc', 'src_sap_ecc__tvm3t')}} AS TVM3T
        ON MVKE.MVGR3 = TVM3T.MVGR3
        AND TVM1T.SPRAS = 'E'
    LEFT JOIN {{source('sap_ecc', 'src_sap_ecc__tvm4t')}} AS TVM4T
        ON MVKE.MVGR4 = TVM4T.MVGR4
        AND TVM4T.SPRAS = 'E'
    LEFT JOIN {{source('sap_ecc', 'src_sap_ecc__tvm5t')}} AS TVM5T
        ON MVKE.MVGR5 = TVM5T.MVGR5
        AND TVM5T.SPRAS = 'E'
QUALIFY ROW_NUMBER()
    OVER(
        PARTITION BY
             MVKE.MATNR
            ,MVKE.DWERK
        ORDER BY 
            MVKE.MATNR ASC
    ) = 1