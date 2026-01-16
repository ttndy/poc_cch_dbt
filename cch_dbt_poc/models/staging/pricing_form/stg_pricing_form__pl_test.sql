-- May Want to unpivot for COMPS
SELECT 
     BASE.PRODUCT                           AS "material"
    ,BASE.DESCRIPTION                       AS "material_description"
    ,BASE.MATERIAL_TYPE                     AS "material_type"
    ,BASE.POWER_SOURCE                      AS "form_of_power"
    ,BASE.CATEGORY_GROUP                    AS "category_group"
    ,BASE.SUB_CATEGORY                      AS "sub_category"
    ,BASE.POWER_DESC                        AS "power_description"
    ,BASE.MFG_SOURCE                        AS "manufacturing_source"
    ,BASE.SHIPPING_TERMS                    AS "shipping_terms"
    ,BASE.RETURN_HANDLING                   AS "return_handling_type" 
    ,BASE.NEW_PRODUCT                       AS "is_new_product"
    ,BASE.TYPE                              AS "submisison_type"
    ,BASE.CUSTOMER                          AS "customer"
    ,BASE.DEPARTMENT                        AS "department"
    ,BASE.CUSTOMER_RETAIL                   AS "new_retail"
    ,BASE.EXISTING_RETAIL                   AS "current_retail"
    ,BASE.INVOICE                           AS "new_invoice"
    ,BASE.EXISTING_INVOICE                  AS "current_invoice"
    ,BASE.OD_STD_COST                       AS "outdoor_standard_cost"
    ,BASE.TTI_GM_PERCENTAGE                 AS "gross_margin_percentage"
    ,BASE.TTI_EBIT_PERCENTAGE               AS "ebit_percentage"
    ,BASE.GROSS_IMU_PERCENTAGE              AS "gross_imu_percentage"
    ,BASE.NET_IMU_PERCENTAGE                AS "net_imu_percentage"
    ,BASE.NOTES                             AS "addtional_notes"
    ,BASE.SCENARIO_YEAR                     AS "scenario_year"
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
