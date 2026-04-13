{{ config(materialized = 'table') }}

WITH source AS (
    SELECT *
    FROM {{ ref('conicet_sedici_imported_items') }}
),

renamed AS (
    SELECT
        NULLIF(TRIM(batch_name), '')::text AS batch_name,
        NULLIF(TRIM(mapfile_item_key), '')::text AS mapfile_item_key,
        sedici_item_id::integer AS sedici_item_id
    FROM source
),

filtered AS (
    SELECT *
    FROM renamed
    WHERE batch_name IS NOT NULL
      AND mapfile_item_key IS NOT NULL
      AND sedici_item_id IS NOT NULL
),

final AS (
    SELECT
        batch_name,
        mapfile_item_key,
        sedici_item_id
    FROM filtered
)

SELECT * FROM final
