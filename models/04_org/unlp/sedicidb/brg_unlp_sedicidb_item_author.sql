{{ config(
    materialized='table',
    indexes=[
        {'columns': ['metadata_value_id'], 'unique': true},
        {'columns': ['item_id']},
        {'columns': ['author_id']},
        {'columns': ['authority_uri']}
    ],
    post_hook=[
        "analyze {{ this }}"
    ]
) }}

WITH author_observation AS (
    SELECT
        item_id,
        metadata_value_id,
        'sedici.creator.person'::text AS metadatafield_fullname,
        'creator_person'::text AS author_role,
        'person'::text AS author_type,
        sedici_creator_person_raw AS author_name_raw,
        sedici_creator_person AS author_name,
        authority_raw,
        authority_uri,
        authority_host,
        authority_path,
        confidence,
        place AS author_place
    FROM {{ ref('int_unlp_sedicidb_item_sedici_creator_person') }}

    UNION ALL

    SELECT
        item_id,
        metadata_value_id,
        'sedici.creator.corporate'::text AS metadatafield_fullname,
        'creator_corporate'::text AS author_role,
        'corporate'::text AS author_type,
        sedici_creator_corporate_raw AS author_name_raw,
        sedici_creator_corporate AS author_name,
        authority_raw,
        authority_uri,
        authority_host,
        authority_path,
        confidence,
        place AS author_place
    FROM {{ ref('int_unlp_sedicidb_item_sedici_creator_corporate') }}

    UNION ALL

    SELECT
        item_id,
        metadata_value_id,
        'sedici.contributor.compiler'::text AS metadatafield_fullname,
        'contributor_compiler'::text AS author_role,
        'person'::text AS author_type,
        sedici_contributor_compiler_raw AS author_name_raw,
        sedici_contributor_compiler AS author_name,
        authority_raw,
        authority_uri,
        authority_host,
        authority_path,
        confidence,
        place AS author_place
    FROM {{ ref('int_unlp_sedicidb_item_sedici_contributor_compiler') }}
),

identified AS (
    SELECT
        item_id,
        metadata_value_id,
        metadatafield_fullname,
        author_role,
        author_type,
        author_name_raw,
        author_name,
        authority_raw,
        authority_uri,
        authority_host,
        authority_path,
        authority_uri IS NOT NULL AS has_authority_control,
        confidence,
        author_place,
        CASE
            WHEN authority_uri IS NOT NULL
                THEN 'authority||' || authority_uri
            ELSE 'name||' || author_type || '||' || LOWER(author_name)
        END AS author_bk
    FROM author_observation
    WHERE author_name IS NOT NULL
),

final AS (
    SELECT
        MD5(author_bk) AS author_id,
        *
    FROM identified
)

SELECT *
FROM final
