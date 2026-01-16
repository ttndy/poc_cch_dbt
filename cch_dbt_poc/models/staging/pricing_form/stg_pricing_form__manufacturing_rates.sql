SELECT 
     BASE.MFG_SOURCE            AS "manufacturing_source"
    ,BASE.ACCOUNT               AS "account"
    ,BASE.PRICING_FORM_ACCOUNT  AS "pricing_form_account"
    ,BASE.SHIPPING_TERMS        AS "shipping_terms"
    ,BASE.VALUE                 AS "value"
    ,BASE.DATE_UPDATED          AS "updated_as_of_date"
    ,BASE.SCENARIO_YEAR         AS "scenario_year"
FROM {{source('pricing_form', 'src_manufacturing_rates')}} AS BASE 