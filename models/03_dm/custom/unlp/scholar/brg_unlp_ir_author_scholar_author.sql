{{ config(materialized='table') }}

WITH ir_author AS (
    SELECT
        ir_author_hk,
        author_name_preferred,
        author_name_normalized,
        author_type,
        authority,
        has_authority_control,
        item_count AS ir_item_count,
        LOWER(
            REGEXP_REPLACE(
                TRANSLATE(
                    author_name_normalized,
                    'ÁÀÂÃÄáàâãäÉÈÊËéèêëÍÌÎÏíìîïÓÒÔÕÖóòôõöÚÙÛÜúùûüÑñÇç',
                    'AAAAAaaaaaEEEEeeeeIIIIiiiiOOOOOoooooUUUUuuuuNnCc'
                ),
                '[^a-z0-9]+',
                ' ',
                'g'
            )
        ) AS author_name_match_key
    FROM {{ ref('dim_unlp_ir_author') }}
    WHERE author_type = 'person'
      AND author_name_normalized IS NOT NULL
),

ir_author_variant AS (
    SELECT DISTINCT
        bridge.ir_author_hk,
        bridge.author_name_raw,
        LOWER(
            REGEXP_REPLACE(
                TRANSLATE(
                    bridge.author_name_normalized,
                    'ÁÀÂÃÄáàâãäÉÈÊËéèêëÍÌÎÏíìîïÓÒÔÕÖóòôõöÚÙÛÜúùûüÑñÇç',
                    'AAAAAaaaaaEEEEeeeeIIIIiiiiOOOOOoooooUUUUuuuuNnCc'
                ),
                '[^a-z0-9]+',
                ' ',
                'g'
            )
        ) AS author_name_match_key
    FROM {{ ref('brg_unlp_ir_item_author') }} AS bridge
    WHERE bridge.author_type = 'person'
      AND bridge.author_name_normalized IS NOT NULL
),

ir_author_enriched AS (
    SELECT
        ir_author.*,
        NULLIF(
            LOWER(
                REGEXP_REPLACE(
                    TRANSLATE(
                        TRIM(
                            CASE
                                WHEN author_name_preferred LIKE '%,%' THEN SPLIT_PART(author_name_preferred, ',', 1)
                                ELSE REGEXP_REPLACE(author_name_preferred, '\s+[^ ]+$', '')
                            END
                        ),
                        'ÁÀÂÃÄáàâãäÉÈÊËéèêëÍÌÎÏíìîïÓÒÔÕÖóòôõöÚÙÛÜúùûüÑñÇç',
                        'AAAAAaaaaaEEEEeeeeIIIIiiiiOOOOOoooooUUUUuuuuNnCc'
                    ),
                    '[^a-z0-9]+',
                    ' ',
                    'g'
                )
            ),
            ''
        ) AS surname_key,
        LEFT(
            NULLIF(
                LOWER(
                    REGEXP_REPLACE(
                        TRANSLATE(
                            TRIM(
                                CASE
                                    WHEN author_name_preferred LIKE '%,%' THEN SPLIT_PART(author_name_preferred, ',', 2)
                                    ELSE SPLIT_PART(author_name_preferred, ' ', 1)
                                END
                            ),
                            'ÁÀÂÃÄáàâãäÉÈÊËéèêëÍÌÎÏíìîïÓÒÔÕÖóòôõöÚÙÛÜúùûüÑñÇç',
                            'AAAAAaaaaaEEEEeeeeIIIIiiiiOOOOOoooooUUUUuuuuNnCc'
                        ),
                        '[^a-z0-9]+',
                        ' ',
                        'g'
                    )
                ),
                ''
            ),
            1
        ) AS given_name_initial,
        ARRAY_LENGTH(
            REGEXP_SPLIT_TO_ARRAY(author_name_match_key, '\s+'),
            1
        ) AS token_count
    FROM ir_author
),

ir_surname_initial_stats AS (
    SELECT
        surname_key,
        given_name_initial,
        COUNT(*)::integer AS ir_author_count_for_surname_initial
    FROM ir_author_enriched
    WHERE surname_key IS NOT NULL
      AND given_name_initial IS NOT NULL
    GROUP BY surname_key, given_name_initial
),

ir_author_unique_surname_initial AS (
    SELECT ir_author_enriched.*
    FROM ir_author_enriched
    INNER JOIN ir_surname_initial_stats AS stats
        ON ir_author_enriched.surname_key = stats.surname_key
       AND ir_author_enriched.given_name_initial = stats.given_name_initial
    WHERE stats.ir_author_count_for_surname_initial = 1
),

scholar_author AS (
    SELECT
        scholar_author_hk,
        scholar_user_id,
        author_name,
        profile_url,
        affiliation,
        verified_email_domain,
        has_unlp_verified_email,
        mentions_unlp_affiliation,
        is_unlp_profile,
        current_cited_by_count,
        LOWER(
            REGEXP_REPLACE(
                TRANSLATE(
                    TRIM(author_name),
                    'ÁÀÂÃÄáàâãäÉÈÊËéèêëÍÌÎÏíìîïÓÒÔÕÖóòôõöÚÙÛÜúùûüÑñÇç',
                    'AAAAAaaaaaEEEEeeeeIIIIiiiiOOOOOoooooUUUUuuuuNnCc'
                ),
                '[^a-z0-9]+',
                ' ',
                'g'
            )
        ) AS author_name_match_key
    FROM {{ ref('dim_unlp_scholar_author') }}
    WHERE author_name IS NOT NULL
),

scholar_author_enriched AS (
    SELECT
        scholar_author.*,
        NULLIF(
            LOWER(
                REGEXP_REPLACE(
                    TRANSLATE(
                        TRIM(
                            CASE
                                WHEN author_name LIKE '%,%' THEN SPLIT_PART(author_name, ',', 1)
                                ELSE REGEXP_REPLACE(author_name, '^.*\s', '')
                            END
                        ),
                        'ÁÀÂÃÄáàâãäÉÈÊËéèêëÍÌÎÏíìîïÓÒÔÕÖóòôõöÚÙÛÜúùûüÑñÇç',
                        'AAAAAaaaaaEEEEeeeeIIIIiiiiOOOOOoooooUUUUuuuuNnCc'
                    ),
                    '[^a-z0-9]+',
                    ' ',
                    'g'
                )
            ),
            ''
        ) AS surname_key,
        LEFT(
            NULLIF(
                LOWER(
                    REGEXP_REPLACE(
                        TRANSLATE(
                            TRIM(
                                CASE
                                    WHEN author_name LIKE '%,%' THEN SPLIT_PART(author_name, ',', 2)
                                    ELSE SPLIT_PART(author_name, ' ', 1)
                                END
                            ),
                            'ÁÀÂÃÄáàâãäÉÈÊËéèêëÍÌÎÏíìîïÓÒÔÕÖóòôõöÚÙÛÜúùûüÑñÇç',
                            'AAAAAaaaaaEEEEeeeeIIIIiiiiOOOOOoooooUUUUuuuuNnCc'
                        ),
                        '[^a-z0-9]+',
                        ' ',
                        'g'
                    )
                ),
                ''
            ),
            1
        ) AS given_name_initial,
        ARRAY_LENGTH(
            REGEXP_SPLIT_TO_ARRAY(author_name_match_key, '\s+'),
            1
        ) AS token_count
    FROM scholar_author
),

scholar_surname_initial_stats AS (
    SELECT
        surname_key,
        given_name_initial,
        COUNT(*)::integer AS scholar_author_count_for_surname_initial
    FROM scholar_author_enriched
    WHERE surname_key IS NOT NULL
      AND given_name_initial IS NOT NULL
    GROUP BY surname_key, given_name_initial
),

scholar_author_unique_surname_initial AS (
    SELECT scholar_author_enriched.*
    FROM scholar_author_enriched
    INNER JOIN scholar_surname_initial_stats AS stats
        ON scholar_author_enriched.surname_key = stats.surname_key
       AND scholar_author_enriched.given_name_initial = stats.given_name_initial
    WHERE stats.scholar_author_count_for_surname_initial = 1
),

match_exact_preferred AS (
    SELECT
        ir.ir_author_hk,
        scholar.scholar_author_hk,
        ir.author_name_preferred AS ir_author_name,
        ir.author_name_normalized AS ir_author_name_normalized,
        scholar.author_name AS scholar_author_name,
        scholar.author_name_match_key AS scholar_author_name_normalized,
        ir.authority,
        ir.has_authority_control,
        ir.ir_item_count,
        scholar.scholar_user_id,
        scholar.profile_url,
        scholar.affiliation,
        scholar.verified_email_domain,
        scholar.has_unlp_verified_email,
        scholar.mentions_unlp_affiliation,
        scholar.is_unlp_profile,
        scholar.current_cited_by_count,
        'exact_preferred_name'::text AS match_method,
        1.0000::numeric(6, 4) AS match_score,
        1 AS match_priority
    FROM ir_author_enriched AS ir
    INNER JOIN scholar_author_enriched AS scholar
        ON ir.author_name_match_key = scholar.author_name_match_key
),

match_exact_variant AS (
    SELECT
        ir.ir_author_hk,
        scholar.scholar_author_hk,
        ir.author_name_preferred AS ir_author_name,
        ir.author_name_normalized AS ir_author_name_normalized,
        scholar.author_name AS scholar_author_name,
        scholar.author_name_match_key AS scholar_author_name_normalized,
        ir.authority,
        ir.has_authority_control,
        ir.ir_item_count,
        scholar.scholar_user_id,
        scholar.profile_url,
        scholar.affiliation,
        scholar.verified_email_domain,
        scholar.has_unlp_verified_email,
        scholar.mentions_unlp_affiliation,
        scholar.is_unlp_profile,
        scholar.current_cited_by_count,
        'exact_observed_variant_name'::text AS match_method,
        0.9800::numeric(6, 4) AS match_score,
        2 AS match_priority
    FROM ir_author_enriched AS ir
    INNER JOIN ir_author_variant AS variant
        USING (ir_author_hk)
    INNER JOIN scholar_author_enriched AS scholar
        ON variant.author_name_match_key = scholar.author_name_match_key
),

match_surname_initial AS (
    SELECT
        ir.ir_author_hk,
        scholar.scholar_author_hk,
        ir.author_name_preferred AS ir_author_name,
        ir.author_name_normalized AS ir_author_name_normalized,
        scholar.author_name AS scholar_author_name,
        scholar.author_name_match_key AS scholar_author_name_normalized,
        ir.authority,
        ir.has_authority_control,
        ir.ir_item_count,
        scholar.scholar_user_id,
        scholar.profile_url,
        scholar.affiliation,
        scholar.verified_email_domain,
        scholar.has_unlp_verified_email,
        scholar.mentions_unlp_affiliation,
        scholar.is_unlp_profile,
        scholar.current_cited_by_count,
        'surname_first_initial_unlp_profile'::text AS match_method,
        0.8500::numeric(6, 4) AS match_score,
        3 AS match_priority
    FROM ir_author_unique_surname_initial AS ir
    INNER JOIN scholar_author_unique_surname_initial AS scholar
        ON ir.surname_key = scholar.surname_key
       AND ir.given_name_initial = scholar.given_name_initial
    WHERE scholar.is_unlp_profile
      AND COALESCE(ir.token_count, 0) >= 2
      AND COALESCE(scholar.token_count, 0) >= 2
      AND LENGTH(COALESCE(ir.surname_key, '')) >= 5
),

matched_union AS (
    SELECT * FROM match_exact_preferred
    UNION ALL
    SELECT * FROM match_exact_variant
),

dedup_pair AS (
    SELECT *
    FROM (
        SELECT
            matched_union.*,
            ROW_NUMBER() OVER (
                PARTITION BY ir_author_hk, scholar_author_hk
                ORDER BY match_priority, match_score DESC, scholar_author_hk
            ) AS pair_rank
        FROM matched_union
    ) AS ranked
    WHERE pair_rank = 1
),

match_stats AS (
    SELECT
        dedup_pair.*,
        COUNT(*) OVER (PARTITION BY ir_author_hk)::integer AS scholar_match_count_for_ir_author,
        COUNT(*) OVER (PARTITION BY scholar_author_hk)::integer AS ir_match_count_for_scholar_author
    FROM dedup_pair
),

final AS (
    SELECT
        ir_author_hk,
        scholar_author_hk,
        ir_author_name,
        ir_author_name_normalized,
        scholar_author_name,
        scholar_author_name_normalized,
        authority,
        has_authority_control,
        ir_item_count,
        scholar_user_id,
        profile_url,
        affiliation,
        verified_email_domain,
        has_unlp_verified_email,
        mentions_unlp_affiliation,
        is_unlp_profile,
        current_cited_by_count,
        match_method,
        match_score,
        scholar_match_count_for_ir_author,
        ir_match_count_for_scholar_author,
        (
            scholar_match_count_for_ir_author = 1
            AND ir_match_count_for_scholar_author = 1
        ) AS is_unique_match
    FROM match_stats
)

SELECT * FROM final
