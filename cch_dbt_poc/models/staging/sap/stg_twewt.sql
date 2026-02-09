SELECT 
     TWEWT.MANDT                    AS "client"
    ,TWEWT.EXTWG                    AS "material_external_group"
    ,TWEWT.EWBEZ                    AS "material_external_group_description"
    ,TWEWT.SOURCE                   AS "sap_instance"
    ,TWEWT.RUN_DATE_TIME_VAR        AS "updated_as_of_date_time"
FROM {{ source('sap_ecc', 'src_sap_ecc__twewt')}} AS TWEWT
WHERE
    TWEWT.SPRAS = 'E'
