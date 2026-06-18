WITH base AS (
    SELECT 
        hub_set.set_id,
        sat_ds.set_name,
        hub_set.set_hk
    FROM {{ ref('hub_oai_set') }} hub_set
    JOIN {{ latest_satellite(ref('sat_oai_set'), 'set_hk', order_column='_load_datetime') }} AS sat_ds USING (set_hk)
)

SELECT * FROM base
