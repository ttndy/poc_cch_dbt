SELECT 
     PAYER                       AS "payer_number"
    ,CUSTOMER                    AS "customer"
FROM {{ source('raw_cch', 'src_cch__dtfo_store_map') }}
