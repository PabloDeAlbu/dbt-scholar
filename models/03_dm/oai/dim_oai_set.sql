WITH base AS (
    SELECT
        hub_set.set_id,
        sat_ds.set_name,
        hub_set.set_hk,
        CASE
            WHEN hub_set.set_id = '!UNKNOWN' THEN 'unknown'
            WHEN hub_set.set_id LIKE 'com\_%' ESCAPE '\' THEN 'community'
            WHEN hub_set.set_id LIKE 'col\_%' ESCAPE '\' THEN 'collection'
            WHEN hub_set.set_id = 'snrd' THEN 'harvest_set'
            ELSE 'other'
        END AS set_type
    FROM {{ ref('hub_oai_set') }} hub_set
    INNER JOIN {{ latest_satellite(ref('sat_oai_set'), 'set_hk', order_column='_load_datetime') }} AS sat_ds
        USING (set_hk)
)

SELECT * FROM base
