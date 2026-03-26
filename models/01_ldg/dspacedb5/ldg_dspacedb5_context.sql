WITH context AS (
    SELECT
        '{{ var("dspacedb5_context_id", "manual_context") }}'::text AS context_id,
        '{{ var("dspacedb5_source_label") }}'::text AS source_label,
        '{{ var("dspacedb5_institution_ror") }}'::text AS institution_ror,
        '{{ var("dspacedb5_extract_datetime") }}'::timestamp AS extract_datetime,
        '{{ var("dspacedb5_load_datetime") }}'::timestamp AS load_datetime
)

SELECT * FROM context
