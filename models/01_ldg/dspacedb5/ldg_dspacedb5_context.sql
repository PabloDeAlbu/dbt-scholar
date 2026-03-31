WITH context AS (
    SELECT
        '{{ var("dspacedb5_source_label", "unknown") }}'::text AS source_label,
        '{{ var("dspacedb5_institution_ror", "https://ror.org/unknown") }}'::text AS institution_ror,
        COALESCE(
            NULLIF('{{ var("dspacedb5_extract_datetime", "") }}', '')::timestamp,
            CURRENT_TIMESTAMP
        ) AS extract_datetime,
        CURRENT_TIMESTAMP AS load_datetime
)

SELECT * FROM context
