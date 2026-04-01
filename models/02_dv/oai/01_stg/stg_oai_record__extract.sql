{{ config(materialized='view') }}

WITH raw_source AS (
    SELECT
        record_id::text AS record_id,
        NULLIF(SPLIT_PART(record_id::text, ':', 2), '') AS repository_identifier,
        '{{ var("oai_institution_ror") }}'::text AS institution_ror,
        COALESCE(extract_datetime::timestamp, _load_datetime::timestamp) AS extract_datetime,
        _load_datetime::timestamp AS load_datetime
    FROM {{ ref('stg_oai_records') }}
    WHERE record_id <> '!UNKNOWN'
),
final AS (
    SELECT
        record_hk,
        record_id,
        repository_identifier,
        institution_ror,
        extract_datetime,
        load_datetime,
        source,
        {{ automate_dv.hash(
            columns=['record_hk', 'extract_datetime', 'repository_identifier'],
            alias='extract_cdk'
        ) }},
        {{ automate_dv.hash(
            columns=['extract_datetime', 'repository_identifier', 'institution_ror'],
            alias='record_extract_hashdiff',
            is_hashdiff=true
        ) }}
    FROM (
        SELECT
            {{ automate_dv.hash(columns='record_id', alias='record_hk') }},
            record_id,
            repository_identifier,
            institution_ror,
            extract_datetime,
            load_datetime,
            '!OAI' AS source
        FROM raw_source
    ) s
)

SELECT * FROM final
