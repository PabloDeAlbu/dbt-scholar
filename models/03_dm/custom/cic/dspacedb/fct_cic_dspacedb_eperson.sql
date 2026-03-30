{{ config(materialized='table') }}

WITH context AS (
    SELECT
        source_label,
        institution_ror,
        extract_datetime,
        load_datetime
    FROM {{ ref('ldg_dspacedb_context') }}
    WHERE institution_key = 'cic'
),

base_eperson AS (
    SELECT
        COALESCE(eperson.eperson_id::text, eperson.uuid::text) AS eperson_id,
        eperson.uuid AS eperson_uuid,
        eperson.email,
        eperson.netid,
        eperson.can_log_in,
        eperson.require_certificate,
        eperson.self_registered,
        context.source_label,
        context.institution_ror,
        context.extract_datetime AS snapshot_extract_datetime,
        context.load_datetime AS snapshot_load_datetime,
        context.institution_ror || '||' || context.source_label || '||' || COALESCE(eperson.eperson_id::text, eperson.uuid::text) AS eperson_bk
    FROM {{ source('dspacedb', 'eperson') }} AS eperson
    CROSS JOIN context
),

submitter_activity AS (
    SELECT
        submitter_id AS eperson_uuid,
        TRUE AS submitter_flag,
        MIN(
            COALESCE(
                last_modified::date,
                first_extract_datetime::date
            )
        ) AS first_item_submission_date,
        MAX(
            COALESCE(
                last_modified::date,
                first_extract_datetime::date
            )
        ) AS last_item_submission_date,
        MIN(first_extract_datetime) AS first_submitter_extract_datetime,
        MAX(last_extract_datetime) AS last_submitter_extract_datetime,
        MIN(first_load_datetime) AS first_submitter_load_datetime,
        MAX(last_load_datetime) AS last_submitter_load_datetime
    FROM {{ ref('fct_cic_dspacedb_item_publication') }}
    -- Este mart institucional no expone una fecha explícita de envío; usamos `last_modified`
    -- como mejor aproximación operativa y caemos a la primera observación en warehouse cuando
    -- el item no tiene fecha útil.
    WHERE submitter_id IS NOT NULL
    GROUP BY submitter_id
),

final AS (
    SELECT
        {{ automate_dv.hash(columns='eperson_bk', alias='eperson_hk') }},
        eperson.eperson_id,
        eperson.email,
        eperson.netid,
        eperson.can_log_in,
        eperson.require_certificate,
        eperson.self_registered,
        COALESCE(submitter.submitter_flag, FALSE) AS submitter_flag,
        submitter.first_item_submission_date,
        submitter.last_item_submission_date,
        eperson.source_label,
        eperson.institution_ror,
        LEAST(
            eperson.snapshot_extract_datetime,
            COALESCE(submitter.first_submitter_extract_datetime, eperson.snapshot_extract_datetime)
        ) AS first_extract_datetime,
        GREATEST(
            eperson.snapshot_extract_datetime,
            COALESCE(submitter.last_submitter_extract_datetime, eperson.snapshot_extract_datetime)
        ) AS last_extract_datetime,
        LEAST(
            eperson.snapshot_load_datetime,
            COALESCE(submitter.first_submitter_load_datetime, eperson.snapshot_load_datetime)
        ) AS first_load_datetime,
        GREATEST(
            eperson.snapshot_load_datetime,
            COALESCE(submitter.last_submitter_load_datetime, eperson.snapshot_load_datetime)
        ) AS last_load_datetime
    FROM base_eperson AS eperson
    LEFT JOIN submitter_activity AS submitter
        USING (eperson_uuid)
)

SELECT * FROM final
