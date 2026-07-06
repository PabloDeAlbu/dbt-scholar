WITH source AS (
  SELECT * FROM {{ source('dspacedb5', 'metadatavalue') }}
),
renamed AS (
  SELECT
    metadata_value_id::integer AS metadata_value_id,
    resource_id::integer AS resource_id,
    metadata_field_id::integer AS metadata_field_id,
    text_value::text AS text_value,
    text_lang::text AS text_lang,
    place::integer AS place,
    authority::text AS authority,
    confidence::integer AS confidence,
    resource_type_id::integer AS resource_type_id
  FROM source
),
ghost_record AS (
  SELECT
    -1::integer AS metadata_value_id,
    -1::integer AS resource_id,
    -1::integer AS metadata_field_id,
    '!UNKNOWN'::text AS text_value,
    '!UNKNOWN'::text AS text_lang,
    -1::integer AS place,
    '!UNKNOWN'::text AS authority,
    -1::integer AS confidence,
    -1::integer AS resource_type_id
)

SELECT * FROM renamed
UNION ALL
SELECT * FROM ghost_record
