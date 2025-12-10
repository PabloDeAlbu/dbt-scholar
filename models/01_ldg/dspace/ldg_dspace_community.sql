WITH base AS (
    SELECT 
        community_id,
        uuid as community_uuid,
        admin,
        logo_bitstream_id,
        {{ dbt_date.today() }} as load_datetime
    FROM {{ source('dspace', 'community') }}
)

SELECT * FROM base