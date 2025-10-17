-- Figure out STAGE of this table from SAM
SELECT *
FROM {{source('pricing_form', 'src_gross_margin_targets')}} AS BASE