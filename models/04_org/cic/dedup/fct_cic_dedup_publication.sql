{{ config(materialized='view') }}

WITH base AS (
    SELECT *
    FROM {{ ref('fct_cic_cicdigital_item_publication') }}
    WHERE discoverable IS TRUE
      AND handle IS NOT NULL
),

author_ranked AS (
    SELECT
        author.item_id,
        author.author_name,
        author.metadata_value_id,
        author.place,
        CASE author.author_role
            WHEN 'author' THEN 1
            WHEN 'corporate' THEN 2
            WHEN 'editor' THEN 3
            WHEN 'compilator' THEN 4
        END AS role_order,
        ROW_NUMBER() OVER (
            PARTITION BY author.item_id, LOWER(author.author_name)
            ORDER BY
                CASE author.author_role
                    WHEN 'author' THEN 1
                    WHEN 'corporate' THEN 2
                    WHEN 'editor' THEN 3
                    WHEN 'compilator' THEN 4
                END,
                author.place NULLS LAST,
                author.metadata_value_id DESC
        ) AS author_rank
    FROM {{ ref('brg_cic_cicdigital_item_author') }} AS author
    INNER JOIN base
        USING (item_id)
),

author_agg AS (
    SELECT
        item_id,
        STRING_AGG(
            author_name,
            '|'
            ORDER BY role_order, place NULLS LAST, metadata_value_id, author_name
        ) AS author
    FROM author_ranked
    WHERE author_rank = 1
    GROUP BY item_id
),

doi_agg AS (
    SELECT
        doi.item_id,
        STRING_AGG(DISTINCT doi.value, '|' ORDER BY doi.value) AS doi
    FROM {{ ref('int_cic_cicdigital_item_dcterms_identifier_doi') }} AS doi
    INNER JOIN base
        USING (item_id)
    GROUP BY doi.item_id
),

isbn_agg AS (
    SELECT
        isbn.item_id,
        STRING_AGG(DISTINCT isbn.value, '|' ORDER BY isbn.value) AS isbn
    FROM {{ ref('int_cic_cicdigital_item_dcterms_identifier_isbn') }} AS isbn
    INNER JOIN base
        USING (item_id)
    GROUP BY isbn.item_id
),

prepared AS (
    SELECT
        'cic_digital'::text AS source,
        base.handle::text AS id,
        base.item_id,
        base.item_url,
        base.dc_title::text AS title,
        base.dcterms_title_subtitle::text AS subtitle,
        CASE
            WHEN LOWER(base.dc_type) = 'parte de libro' THEN 'bookpart'
            WHEN LOWER(base.cic_parent_type) IN ('articulo', 'artículo') THEN 'article'
            WHEN LOWER(base.cic_parent_type) = 'libro' THEN 'book'
            WHEN LOWER(base.cic_parent_type) = 'objeto de conferencia'
                THEN 'objeto de conferencia'
            ELSE 'unknown'
        END::text AS type,
        base.dc_type::text AS type_raw,
        base.cic_parent_type::text AS subtype,
        authors.author::text AS author,
        base.dcterms_issued AS date,
        doi.doi::text AS doi,
        isbn.isbn::text AS isbn,
        NULL::text AS issn,
        NULL::text AS subject,
        base.dcterms_description::text AS description,
        base.owning_collection_id::text AS owning_collection_id,
        base.owning_collection_title::text AS owning_collection_title,
        base.owning_community_id::text AS owning_community_id,
        base.owning_community_title::text AS owning_community_title
    FROM base
    LEFT JOIN author_agg AS authors
        USING (item_id)
    LEFT JOIN doi_agg AS doi
        USING (item_id)
    LEFT JOIN isbn_agg AS isbn
        USING (item_id)
),

final AS (
    SELECT
        *,
        EXTRACT(YEAR FROM date)::integer AS publication_year,
        (
            title IS NOT NULL
            AND type <> 'unknown'
            AND author IS NOT NULL
            AND date IS NOT NULL
        ) AS dedup_eligible
    FROM prepared
)

SELECT * FROM final
