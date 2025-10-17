SELECT 
     BASE.CUST_GROUP             AS "customer_group"
    ,BASE.DEPARTMENT             AS "department"
    ,BASE.PRICING_FORM_ACCOUNT   AS "pricing_form_account"
    ,BASE.DRIVER                 AS "driver_account"
    ,BASE.ACCOUNT                AS "cch__account"
    ,BASE.VALUE                  AS "value"
    ,BASE.DATE_UPDATED           AS "updated_as_of_date" 
FROM {{source('pricing_form', 'src_base_drivers')}} AS BASE
