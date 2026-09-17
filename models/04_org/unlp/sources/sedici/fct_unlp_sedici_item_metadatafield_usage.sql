{{ config(
    materialized='table',
    indexes=[
        {'columns': ['item_hk', 'metadatafield_hk'], 'unique': true},
        {'columns': ['metadatafield_fullname', 'item_hk']}
    ],
    post_hook=[
        "analyze {{ this }}"
    ]
) }}

WITH base AS (
    SELECT
        item_hk
    FROM {{ ref('fct_dspacedb5_item_publication') }}
    WHERE institution_ror = 'https://ror.org/01tjs6929'
      AND discoverable = TRUE
      AND in_archive = TRUE
      AND withdrawn = FALSE
),

final AS (
    SELECT
        usage.item_hk,
        usage.item_id,
        usage.source_label,
        usage.institution_ror,
        usage.metadatafield_hk,
        usage.metadatafield_fullname,
        usage.metadatafield_name,
        usage.short_id,
        usage.metadata_field_id,
        usage.element,
        usage.qualifier,
        usage.metadata_value_count,
        usage.metadata_value_id_count,
        usage.distinct_nonempty_text_value_count,
        usage.preferred_text_value,
        usage.min_text_value,
        usage.min_ymd_text_value,
        usage.min_ym_text_value,
        usage.min_year_text_value,
        usage.ordered_text_values,
        usage.first_load_datetime,
        usage.last_load_datetime
    FROM {{ ref('fct_dspacedb5_item_metadatafield_usage') }} AS usage
    INNER JOIN base
        USING (item_hk)
)

SELECT *
FROM final
