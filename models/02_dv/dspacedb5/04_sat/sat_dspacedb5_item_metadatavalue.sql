{{ config(materialized='incremental') }}

WITH staged_source AS (
    SELECT
        item_bk,
        metadatavalue_bk,
        metadatafield_bk,
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
    WHERE item_bk IS NOT NULL
      AND metadatavalue_bk IS NOT NULL
      AND metadatafield_bk IS NOT NULL
),
hashed_source AS (
    SELECT
        s0.item_id,
        s0.metadata_value_id,
        s0.metadata_field_id,
        s0.text_value,
        s0.text_lang,
        s0.place,
        s0.authority,
        s0.confidence,
        s0.source_label,
        s0.institution_ror,
        s0.effective_from,
        s0.load_datetime,
        s0.source,
        s1.item_hk,
        s1.metadatavalue_hk,
        s1.metadatafield_hk,
        s1.item_metadatavalue_hk,
        s1.hashdiff
    FROM staged_source AS s0
    CROSS JOIN LATERAL (
        SELECT
            {{ automate_dv.hash(columns='item_bk', alias='item_hk') }},
            {{ automate_dv.hash(columns='metadatavalue_bk', alias='metadatavalue_hk') }},
            {{ automate_dv.hash(columns='metadatafield_bk', alias='metadatafield_hk') }},
            {{ automate_dv.hash(columns=['item_bk', 'metadatavalue_bk'], alias='item_metadatavalue_hk') }},
            {{ automate_dv.hash(
                columns=[
                    'item_bk',
                    'metadatavalue_bk',
                    'metadatafield_bk',
                    'text_value',
                    'text_lang',
                    'place',
                    'authority',
                    'confidence'
                ],
                alias='hashdiff',
                is_hashdiff=true
            ) }}
    ) AS s1
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
            FROM hashed_source
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
            hs.*,
            LAG(
                hs.hashdiff,
                1,
                {% if is_incremental() %}
                COALESCE(lr.hashdiff, {{ automate_dv.cast_binary('FFFFFFFF', quote=true) }})
                {% else %}
                {{ automate_dv.cast_binary('FFFFFFFF', quote=true) }}
                {% endif %}
            ) OVER (
                PARTITION BY hs.item_metadatavalue_hk
                ORDER BY hs.load_datetime ASC, hs.effective_from ASC
            ) AS prev_hashdiff
        FROM hashed_source AS hs
        {% if is_incremental() %}
        LEFT JOIN latest_records AS lr
            USING (item_metadatavalue_hk)
        {% endif %}
    ) AS b
    WHERE b.hashdiff != b.prev_hashdiff
)

SELECT *
FROM unique_source_records
