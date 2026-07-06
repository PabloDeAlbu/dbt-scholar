WITH source AS (
  SELECT * FROM {{ source('dspacedb5', 'community2community') }}
),
renamed AS (
  SELECT
    id::bigint AS community_community_id,
    parent_comm_id::integer AS parent_comm_id,
    child_comm_id::integer AS child_comm_id
  FROM source
),
ghost_record AS (
  SELECT
    -1::bigint AS community_community_id,
    -1::integer AS parent_comm_id,
    -1::integer AS child_comm_id
)

SELECT * FROM renamed
UNION ALL
SELECT * FROM ghost_record
