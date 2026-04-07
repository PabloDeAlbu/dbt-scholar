{{ config(materialized='table') }}

WITH conflicting_keys AS (
    SELECT
        metadatafield_fullname,
        source_label,
        institution_ror,
        COUNT(*) AS issue_count
    FROM {{ ref('dim_dspacedb5_metadatafield') }}
    GROUP BY metadatafield_fullname, source_label, institution_ror
    HAVING COUNT(*) > 1
),
final AS (
    SELECT
        d.metadatafield_fullname,
        d.short_id,
        d.element,
        d.qualifier,
        d.source_label,
        d.institution_ror,
        d.metadatafield_hk,
        c.issue_count
    FROM {{ ref('dim_dspacedb5_metadatafield') }} d
    JOIN conflicting_keys c
        USING (metadatafield_fullname, source_label, institution_ror)
)

SELECT * FROM final
