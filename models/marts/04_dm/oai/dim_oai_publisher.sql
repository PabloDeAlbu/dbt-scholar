WITH base AS (
    SELECT 
        hub_publisher.publishers,
        hub_publisher.publisher_hk
    FROM {{ ref('hub_oai_publisher') }} hub_publisher
)

SELECT * FROM base
