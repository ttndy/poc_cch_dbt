SELECT 
     BASE.CATEGORY           AS "category"
    ,BASE.VALUE              AS "value"
    ,BASE.DATE_UPDATED       AS "update_as_of_date"
    ,'2025ACT'               AS "scenario"
FROM {{source('pricing_form', 'src_dutty_rates')}} AS BASE 
