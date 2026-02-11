SELECT 
     COPA.PERIO                  AS "period_raw"
    ,LEFT(COPA.PERIO, 4)
        || 'ACT'                 AS "scenario"
    ,RIGHT(COPA.PERIO,2)         AS "period"
    ,COPA.WERKS                  AS "plant"
    ,COPA.BUKRS                  AS "company_code"
    ,COPA.VKORG                  AS "sales_org"
    ,COPA.FKART                  AS "billing_type"
    ,COPA.VTWEG                  AS "distrubtion_channel"
    ,COPA.KAUFN                  AS "sales_order_number"
    ,COPA.KUNRG                  AS "payer_number"
    ,COPA.KNDNR                  AS "customer"
    ,COPA.ARTNR                  AS "material"    
    ,COPA.VVQTY                  AS "quantity"
    ,COPA.VVREV                  AS "sales_revenue"
    ,COPA.VVDED                  AS "sales_deductions"  -- HART
    ,COPA.VVALW                  AS "sales_allowances"
    ,COPA.VVDSC                  AS "cash_discounts"
    ,COPA.VV902                  AS "market_fee"
    ,COPA.VVFRE                  AS "freight_expense"
    ,COPA.VV901                  AS "defective_allowance"
    ,COPA.VVMAT                  AS "material_cogs"
    ,COPA.VVTOT                  AS "total_cogs"
    ,COPA.WRTTP                  AS "WRTTP"         -- NO Idea what this is 
FROM {{source('sap_ecc', 'src_sap_ecc__copa_4')}} AS COPA

