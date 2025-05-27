WITH base AS (
    SELECT 
        community_id,
        uuid,
        admin,
        logo_bitstream_id,
        {{ dbt_date.today() }} as load_datetime
    FROM {{ source('dspace7', 'community') }}
)

SELECT * FROM base