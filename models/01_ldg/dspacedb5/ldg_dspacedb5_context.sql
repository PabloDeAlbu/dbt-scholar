WITH repository_map AS (
    SELECT
        repository_key,
        institution_key,
        source_label,
        institution_ror,
        is_active
    FROM {{ ref('seed_dspacedb5_repository') }}
),

selected_repository AS (
    SELECT
        repository_key,
        institution_key,
        source_label,
        institution_ror
    FROM repository_map
    WHERE (
        '{{ var("dspacedb5_repository_key", "") }}' <> ''
        AND repository_key = '{{ var("dspacedb5_repository_key", "") }}'
    ) OR (
        '{{ var("dspacedb5_repository_key", "") }}' = ''
        AND is_active = true
    )
),

context AS (
    SELECT
        repository_key AS context_id,
        institution_key,
        source_label,
        institution_ror,
        COALESCE(
            NULLIF('{{ var("dspacedb5_extract_datetime", "") }}', '')::timestamp,
            CURRENT_TIMESTAMP
        ) AS extract_datetime,
        CURRENT_TIMESTAMP AS load_datetime
    FROM selected_repository
)

SELECT * FROM context
