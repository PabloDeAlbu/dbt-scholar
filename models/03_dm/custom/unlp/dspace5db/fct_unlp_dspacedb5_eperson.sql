{{ config(materialized='table') }}

WITH context AS (
    SELECT
        source_label,
        institution_ror,
        extract_datetime,
        load_datetime
    FROM {{ ref('ldg_dspacedb5_context') }}
    WHERE institution_key = 'unlp'
),

base_eperson AS (
    SELECT
        eperson.eperson_id,
        eperson.email,
        eperson.netid,
        eperson.can_log_in,
        eperson.require_certificate,
        eperson.self_registered,
        context.source_label,
        context.institution_ror,
        context.extract_datetime AS snapshot_extract_datetime,
        context.load_datetime AS snapshot_load_datetime,
        context.institution_ror || '||' || context.source_label || '||' || eperson.eperson_id::text AS eperson_bk
    FROM {{ source('dspacedb5', 'eperson') }} AS eperson
    CROSS JOIN context
),

submitter_activity AS (
    SELECT
        submitter_id AS eperson_id,
        TRUE AS submitter_flag,
        MIN(
            COALESCE(
                dc_date_available,
                last_modified::date,
                dc_date_issued,
                first_extract_datetime::date
            )
        ) AS first_item_submission_date,
        MAX(
            COALESCE(
                dc_date_available,
                last_modified::date,
                dc_date_issued,
                first_extract_datetime::date
            )
        ) AS last_item_submission_date,
        MIN(first_extract_datetime) AS first_submitter_extract_datetime,
        MAX(last_extract_datetime) AS last_submitter_extract_datetime,
        MIN(first_load_datetime) AS first_submitter_load_datetime,
        MAX(last_load_datetime) AS last_submitter_load_datetime
    FROM {{ ref('fct_unlp_dspacedb5_item_publication') }}
    -- DSpace no expone en este mart una fecha explícita de envío; usamos `dc_date_available`
    -- como mejor aproximación y caemos a `last_modified`, `dc_date_issued` y luego la
    -- primera observación en warehouse si el item no trae fechas de negocio útiles.
    WHERE submitter_id IS NOT NULL
      AND submitter_id <> -1
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
        USING (eperson_id)
)

SELECT * FROM final
