{{ config(materialized='view') }}

WITH base AS (
    SELECT
        item_hk,
        dc_identifier_uri,
        date_issued,
        subject,
        author,
        type,
        title,
        subtitle,
        issn,
        doi,
        handle,
        isbn
    FROM {{ ref('fct_unlp_ir_item_publication') }}
),

final AS (
    SELECT
        base.dc_identifier_uri AS id,
        base.date_issued::text AS date,
        base.type,
        {{ clean_text('base.author') }} AS author,
        CASE
            WHEN NULLIF(base.doi, '') IS NOT NULL
                THEN 'https://doi.org/' || REPLACE(base.doi, '|', '|https://doi.org/')
        END AS doi,
        base.doi AS doi_raw,
        base.isbn,
        base.issn,
        {{ clean_text('base.subject') }} AS subject,
        {{ clean_text('base.title') }} AS title,
        {{ clean_text('base.subtitle') }} AS subtitle,
        CASE
            WHEN NULLIF(TRIM(base.subtitle), '') IS NOT NULL
                THEN CONCAT(base.title, ': ', base.subtitle)
            ELSE base.title
        END AS title_subtitle
    FROM base
)

SELECT * FROM final
