SELECT 
     "material"
    ,"material_description"
    ,"submisison_type"
    ,"customer"
    ,"department"
    ,"new_invoice"
    ,"new_retail"
    ,"standard_cost"
    ,"pricing_form_account"
    ,"rate"
    ,"driver"
    ,"rate_scenario"
    ,"mat_oh_cost"
FROM (
    SELECT *
    FROM {{ ref('int_pricing_form__manufacturing')}} AS MFG_RATES
    
    UNION ALL -------------------------------------------------------
    
    SELECT *
    FROM {{ ref('int_pricing_form__base_drivers')}} AS BASE_DRIVERS
    
    UNION ALL -------------------------------------------------------
    
    SELECT *  
    FROM {{ ref('int_pricing_form__sga_rates')}} AS SGA
    
    UNION ALL -------------------------------------------------------
    
    SELECT *  
    FROM {{ ref('int_pricing_form__waranty')}} AS WARRANTY 
)
