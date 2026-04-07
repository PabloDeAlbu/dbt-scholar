WITH context AS (
    SELECT
        REGEXP_REPLACE(
            LOWER(TRIM('{{ required_var("dspacedb5_base_url") }}'::text)),
            '^https?://',
            ''
        ) AS base_url,
        REGEXP_REPLACE(
            LOWER(TRIM('{{ required_var("dspacedb5_base_url") }}'::text)),
            '^https?://',
            ''
        ) AS source_label,
        '{{ required_var("dspacedb5_institution_ror") }}'::text AS institution_ror,
        COALESCE(
            NULLIF('{{ var("dspacedb5_extract_datetime", "") }}', '')::timestamp,
            CURRENT_DATE::timestamp
        ) AS extract_datetime,
        COALESCE(
            NULLIF('{{ var("dspacedb5_load_datetime", "") }}', '')::timestamp,
            CURRENT_DATE::timestamp
        ) AS load_datetime
)

SELECT * FROM context
