{{ config(materialized='view') }}

WITH ranked AS (
    SELECT
        publication.*,
        ROW_NUMBER() OVER (
            PARTITION BY publication.item_uuid
            ORDER BY
                publication.last_extract_datetime DESC NULLS LAST,
                publication.last_load_datetime DESC NULLS LAST,
                publication.first_extract_datetime DESC NULLS LAST,
                publication.base_url,
                publication.source_label
        ) AS item_rank
    FROM {{ ref('fct_dspacedb_item_publication') }} AS publication
    WHERE publication.institution_ror = 'https://ror.org/02s7sax82'
)

SELECT
    item_hk,
    item_id,
    item_uuid,
    submitter_id,
    in_archive,
    withdrawn,
    discoverable,
    owning_collection,
    last_modified,
    base_url,
    source_label,
    institution_ror,
    first_extract_datetime,
    last_extract_datetime,
    first_load_datetime,
    last_load_datetime
FROM ranked
WHERE item_rank = 1
