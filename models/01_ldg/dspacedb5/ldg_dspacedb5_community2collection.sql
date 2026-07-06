WITH source AS (
  SELECT * FROM {{ source('dspacedb5', 'community2collection') }}
),
renamed AS (
  SELECT
    id::bigint AS community_collection_id,
    community_id::integer AS community_id,
    collection_id::integer AS collection_id
  FROM source
),
ghost_record AS (
  SELECT
    -1::bigint AS community_collection_id,
    -1::integer AS community_id,
    -1::integer AS collection_id
)

SELECT * FROM renamed
UNION ALL
SELECT * FROM ghost_record
