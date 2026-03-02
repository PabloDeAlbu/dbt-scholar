WITH source AS (
  SELECT * FROM {{ source('openaire', 'researchproduct_alternateidentifiers') }}
),

renamed AS (
  SELECT
    id::text as researchproduct_id,
    scheme::text,
    value::text,
    _load_datetime::timestamp
  FROM source
),

ghost_record AS (
  SELECT
    '!UNKNOWN'::text as researchproduct_id,
    '!UNKNOWN'::text as scheme,
    '!UNKNOWN'::text as value,
    {{ dbt_date.today() }} as _load_datetime
)

SELECT * FROM renamed
union all
SELECT * FROM ghost_record
