-- May Want to unpivot for COMPS
SELECT 
     BASE.PRODUCT                           AS "material"
    ,BASE.DESCRIPTION                       AS "material_description"
    ,BASE.MATERIAL_TYPE                     AS "material_type"
    ,BASE.POWER_SOURCE                      AS "form_of_power"
    ,BASE.CATEGORY_GROUP                    AS "category_group"
    ,BASE.SUB_CATEGORY                      AS "sub_categorty"
    ,BASE.POWER_DESC                        AS "power_description"
    ,BASE.MFG_SOURCE                        AS "manufacturing_source"
    ,'Domestic'                             AS "shipping_terms"
    ,'Out for Repair'                       AS "return_hadling_type" -- Fix in upload
    ,BASE.NEW_PRODUCT                       AS "is_new_product"
    ,BASE.TYPE                              AS "submisison_type"
    ,BASE.CUSTOMER                          AS "customer"
    ,BASE.DEPARTMENT                        AS "department"
    ,BASE.CUSTOMER_RETAIL                   AS "new_retail"
    ,BASE.EXISTING_RETAIL                   AS "current_retail"
    ,BASE.INVOICE                           AS "new_invoice"
    ,BASE.EXISTING_INVOICE                  AS "current_invoice"
    ,BASE.OD_STD_COST                       AS "outdoor_standard_cost"
    ,BASE.TTI_GM_PRECENTAGE                 AS "gross_margin_percentage"
    ,BASE.TTI_EBIT_PRECENTAGE               AS "ebit_percentage"
    ,BASE.GROSS_IMU_PRECENTAGE              AS "gross_imu_percentage"
    ,BASE.NET_IMU_PRECENTAGE                AS "net_imu_percentage"
    ,BASE.NOTES                             AS "addtional_notes"
    ,DATEADD(
         day
        ,CAST(BASE.START_DATE AS INT) -
            CASE 
                WHEN START_DATE != 0 
                    THEN 2 
                ELSE 
                    0
                END
        ,'1900-01-01'
    )                                        AS "start_date"
    ,DATEADD(
         day
        ,CAST(BASE.END_DATE AS INT) - 2
        ,'1900-01-01'
    )                                        AS "end_date"
FROM {{source('pricing_form', 'src_pricing_pl_test_data')}} AS BASE
