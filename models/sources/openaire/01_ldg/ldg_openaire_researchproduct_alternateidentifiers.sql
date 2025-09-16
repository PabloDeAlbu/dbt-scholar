WITH source AS (
  SELECT * FROM {{ source('openaire', 'researchproduct_alternateidentifiers') }}
),

renamed AS (
  SELECT
    id::text as researchproduct_id,
    scheme::text,
    value::text,
    load_datetime::timestamp
  FROM source
)

SELECT * FROM renamed
