{{ config(materialized='incremental') }}

WITH staged_source AS (
    SELECT
        item_hk,
        metadatavalue_hk,
        metadatafield_hk,
        item_metadatavalue_hk,
        item_metadatavalue_hashdiff AS hashdiff,
        item_id,
        metadata_value_id,
        metadata_field_id,
        text_value,
        text_lang,
        place,
        authority,
        confidence,
        source_label,
        institution_ror,
        effective_from,
        load_datetime,
        source
    FROM {{ ref('stg_dspacedb5_item_metadatavalue') }}
    WHERE item_hk IS NOT NULL
      AND metadatavalue_hk IS NOT NULL
      AND metadatafield_hk IS NOT NULL
      AND item_metadatavalue_hk IS NOT NULL
),
{% if is_incremental() %}
latest_records AS (
    SELECT
        current_records.item_metadatavalue_hk,
        current_records.hashdiff,
        current_records.load_datetime
    FROM (
        SELECT
            current_records.item_metadatavalue_hk,
            current_records.hashdiff,
            current_records.load_datetime,
            ROW_NUMBER() OVER (
                PARTITION BY current_records.item_metadatavalue_hk
                ORDER BY current_records.load_datetime DESC
            ) AS rank
        FROM {{ this }} AS current_records
        JOIN (
            SELECT DISTINCT item_metadatavalue_hk
            FROM staged_source
        ) AS source_records
            USING (item_metadatavalue_hk)
    ) AS current_records
    WHERE rank = 1
),
{% endif %}
unique_source_records AS (
    SELECT
        b.item_metadatavalue_hk,
        b.hashdiff,
        b.item_hk,
        b.item_id,
        b.metadatavalue_hk,
        b.metadatafield_hk,
        b.metadata_value_id,
        b.metadata_field_id,
        b.text_value,
        b.text_lang,
        b.place,
        b.authority,
        b.confidence,
        b.source_label,
        b.institution_ror,
        b.effective_from,
        b.load_datetime,
        b.source
    FROM (
        SELECT
            ss.*,
            LAG(
                ss.hashdiff,
                1,
                {% if is_incremental() %}
                COALESCE(lr.hashdiff, {{ automate_dv.cast_binary('FFFFFFFF', quote=true) }})
                {% else %}
                {{ automate_dv.cast_binary('FFFFFFFF', quote=true) }}
                {% endif %}
            ) OVER (
                PARTITION BY ss.item_metadatavalue_hk
                ORDER BY ss.load_datetime ASC, ss.effective_from ASC
            ) AS prev_hashdiff
        FROM staged_source AS ss
        {% if is_incremental() %}
        LEFT JOIN latest_records AS lr
            USING (item_metadatavalue_hk)
        {% endif %}
    ) AS b
    WHERE b.hashdiff != b.prev_hashdiff
)

SELECT *
FROM unique_source_records
