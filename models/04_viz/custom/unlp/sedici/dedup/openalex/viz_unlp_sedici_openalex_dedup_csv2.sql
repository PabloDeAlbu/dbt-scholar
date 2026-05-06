{{ config(materialized='view') }}

WITH base AS (
    SELECT
        item_hk,
        {{ clean_text('dc_identifier_uri') }} AS dc_identifier_uri,
        {{ clean_text('date_issued') }} AS date_issued,
        {{ clean_text('subject') }} AS subject,
        {{ clean_text('author') }} AS author,
        {{ clean_text('type') }} AS type,
        {{ clean_text('title') }} AS title,
        {{ clean_text('subtitle') }} AS subtitle,
        {{ clean_text('issn') }} AS issn,
        {{ clean_text('doi') }} AS doi,
        {{ clean_text('handle') }} AS handle,
        {{ clean_text('isbn') }} AS isbn
    FROM {{ ref('fct_unlp_ir_item_publication') }}
),

final AS (
    SELECT
        dc_identifier_uri AS id,
        date_issued AS date,
        type,
        author,

        CASE
            WHEN NULLIF(doi, '') IS NOT NULL
                THEN 'https://doi.org/' || REPLACE(doi, '|', '|https://doi.org/')
            ELSE ''
        END AS doi,

        doi AS doi_raw,
        isbn,
        issn,
        subject,
        title,
        subtitle,

        CASE
            WHEN NULLIF(subtitle, '') IS NOT NULL
                THEN CONCAT(title, ': ', subtitle)
            ELSE title
        END AS title_subtitle

    FROM base
)

SELECT * FROM final