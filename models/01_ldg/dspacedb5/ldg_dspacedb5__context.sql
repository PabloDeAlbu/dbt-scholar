WITH context AS (
    SELECT
        REGEXP_REPLACE(
            LOWER(TRIM('{{ required_var("dspacedb5_base_url") }}'::text)),
            '^https?://',
            ''
        )::text AS base_url,
        REGEXP_REPLACE(
            LOWER(TRIM('{{ required_var("dspacedb5_base_url") }}'::text)),
            '^https?://',
            ''
        )::text AS source_label,
        '{{ required_var("dspacedb5_institution_ror") }}'::text AS institution_ror,
        '{{ required_var("dspacedb5_extract_datetime") }}'::timestamp AS extract_datetime,
        '{{ required_var("dspacedb5_load_datetime") }}'::timestamp AS load_datetime
)

SELECT *
FROM context
