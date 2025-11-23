WITH base AS (
    SELECT 
        hub_ds.set_id,
        sat_ds.name
    FROM {{ ref('hub_oai_set') }} hub_ds
    JOIN {{ latest_satellite(ref('sat_oai_set'), 'set_hk') }} AS sat_ds USING (set_hk)
)

SELECT * FROM base
