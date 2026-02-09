SELECT 
     ADV.GL             AS "general_ledger_acocunt"
    ,ADV."Description"  AS "general_ledger_acocunt_description"
FROM {{source('raw_cch','src_cch_advertising_accounts')}} ADV
