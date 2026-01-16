SELECT 
     BASE.TARIFF_TYPE            AS "tarrif_type"
    ,BASE.VALUE                  AS "value"
    ,BASE.DATE_UPDATED           AS "udpated_as_of_date"
    ,BASE.SCENARIO_YEAR          AS "scenario_year"
FROM {{source('pricing_form', 'src_tarrif_types')}} AS BASE