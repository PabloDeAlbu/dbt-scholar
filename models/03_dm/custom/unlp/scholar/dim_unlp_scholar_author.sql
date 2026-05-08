{{ config(materialized='table') }}

WITH author_history AS (
    SELECT
        scholar_user_id,
        MIN(load_datetime) AS first_seen_datetime,
        MAX(load_datetime) AS last_seen_datetime,
        COUNT(*)::integer AS observation_count,
        MIN(cited_by_count) AS min_cited_by_count,
        MAX(cited_by_count) AS max_cited_by_count
    FROM {{ ref('ldg_unlp_scholar_author') }}
    GROUP BY scholar_user_id
),

ranked_author AS (
    SELECT
        author.*,
        ROW_NUMBER() OVER (
            PARTITION BY scholar_user_id
            ORDER BY load_datetime DESC, source_file DESC, profile_url DESC
        ) AS observation_rank
    FROM {{ ref('ldg_unlp_scholar_author') }} AS author
),

latest_author AS (
    SELECT *
    FROM ranked_author
    WHERE observation_rank = 1
),

final AS (
    SELECT
        {{ automate_dv.hash(columns='latest_author.scholar_user_id', alias='scholar_author_hk') }},
        latest_author.scholar_user_id,
        latest_author.author_name,
        latest_author.profile_url,
        latest_author.affiliation,
        latest_author.verified_email,
        latest_author.verified_email_domain,
        latest_author.has_unlp_verified_email,
        latest_author.mentions_unlp_affiliation,
        (latest_author.has_unlp_verified_email OR latest_author.mentions_unlp_affiliation) AS is_unlp_profile,
        latest_author.cited_by_count AS current_cited_by_count,
        author_history.min_cited_by_count,
        author_history.max_cited_by_count,
        CASE
            WHEN author_history.min_cited_by_count IS NULL
                OR latest_author.cited_by_count IS NULL THEN NULL
            ELSE latest_author.cited_by_count - author_history.min_cited_by_count
        END AS cited_by_growth_since_first_seen,
        latest_author.interests_json,
        latest_author.interests,
        latest_author.interest_count,
        author_history.observation_count,
        author_history.first_seen_datetime,
        author_history.last_seen_datetime,
        latest_author.source_system,
        latest_author.entity_type,
        latest_author.source_file AS latest_source_file
    FROM latest_author
    INNER JOIN author_history
        USING (scholar_user_id)
)

SELECT * FROM final
