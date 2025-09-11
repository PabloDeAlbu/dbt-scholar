WITH source AS (
  SELECT * FROM {{ source('openaire', 'researchproduct_alternateidentifiers') }}
),

renamed AS (
  SELECT
    id::text,
    scheme::text,
    value::text,
    load_datetime::timestamp
  FROM source
)

SELECT * FROM renamed
