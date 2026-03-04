SELECT 
     BASE.PRODUCT                           AS "material"
    ,BASE.DESCRIPTION                       AS "material_description"
    ,BASE.VER                               AS "version"

    ,BASE.MATERIAL_TYPE                     AS "material_type"
    ,BASE.POWER_SOURCE                      AS "form_of_power"
    ,BASE.CATEGORY_GROUP                    AS "category_group"
    ,BASE.SUB_CATEGORY                      AS "sub_category"
    ,BASE.POWER_DESC                        AS "power_description"
    ,BASE.MFG_SOURCE                        AS "manufacturing_source"
    ,BASE.TRANSITION_SKU                    AS "transition_sku"

    ,BASE.SHIPPING_TERMS                    AS "shipping_terms"
    ,BASE.RETURN_HANDLING                   AS "return_handling_type" 
    ,BASE.NEW_PRODUCT                       AS "is_new_product"
    ,BASE.TYPE                              AS "submission_type"

    ,BASE.CUSTOMER                          AS "customer"
    ,BASE.DEPARTMENT                        AS "department"

    ,BASE.CUSTOMER_RETAIL                   AS "new_retail"
    ,BASE.EXISTING_RETAIL                   AS "current_retail"
    ,BASE.VOLUME                            AS "volume"

    ,BASE.INVOICE                           AS "new_invoice"
    ,BASE.EXISTING_INVOICE                  AS "current_invoice"
    ,BASE.OD_STD_COST                       AS "outdoor_standard_cost"

    ,BASE.TARIFF_PERCENTAGE                 AS "tariff_percentage"
    ,BASE.TTI_GM_PERCENTAGE                 AS "gross_margin_percentage"
    ,BASE.TTI_EBIT_PERCENTAGE               AS "ebit_percentage"
    ,BASE.GROSS_IMU_PERCENTAGE              AS "gross_imu_percentage"
    ,BASE.NET_IMU_PERCENTAGE                AS "net_imu_percentage"

    ,BASE.SPECIAL_PROMO                     AS "special_promo_cost"
    ,BASE.RD                                AS "rd_cost"
    ,BASE.DISTRIBUTION                      AS "distribution_cost"

    ,BASE.MATERIAL_LAB_OH                   AS "material_overhead_cost"

    -- Removed transport_cost, changing it to 
    ,(4600 + BASE.ADDITIONAL_FREIGHT) 
        / NULLIF(BASE.CONTAINER_QTY, 0)     AS "transport_cost"

    ,BASE.CONTAINER_QTY                     AS "container_quantity"
    ,BASE.ADDITIONAL_FREIGHT                AS "additional_freight"

    ,BASE.START_DATE                        AS "start_date"
    ,BASE.END_DATE                          AS "end_date"
    ,BASE.SCENARIO_YEAR                     AS "scenario_year"

    ,BASE.NOTES                             AS "additional_notes"

    -- ,BASE.CUSTOM_1
    -- ,BASE.CUSTOM_2
    -- ,BASE.CUSTOM_3
    -- ,BASE.CUSTOM_4
    -- ,BASE.CUSTOM_5
    -- ,BASE.CUSTOM_6
    -- ,BASE.CUSTOM_7
    -- ,BASE.CUSTOM_8
    -- ,BASE.CUSTOM_9
    -- ,BASE.CUSTOM_10

FROM {{source('pricing_form', 'src_pricing_pl_test_data')}} AS BASE