{{ config(materialized='view') }}

{%- set yaml_metadata -%}
source_model: ldg_dspacedb5_metadatavalue
derived_columns:
  item_id: resource_id
  metadatavalue_bk: "institution_ror || '||' || source_label || '||' || base_url || '||' || metadata_value_id::text"
  metadatafield_bk: "institution_ror || '||' || source_label || '||' || base_url || '||' || metadata_field_id::text"
  item_bk: "institution_ror || '||' || source_label || '||' || base_url || '||' || resource_id::text"
  source: source_label
  effective_from: extract_datetime
  start_date: extract_datetime
  end_date: "TO_DATE('9999-12-31', 'YYYY-MM-DD')"
hashed_columns:
  item_hk: item_bk
  metadatavalue_hk: metadatavalue_bk
  metadatafield_hk: metadatafield_bk
  item_metadatavalue_hk:
    - item_bk
    - metadatavalue_bk
  item_metadatavalue_hashdiff:
    is_hashdiff: true
    columns:
      - item_bk
      - metadatavalue_bk
      - metadatafield_bk
      - text_value
      - text_lang
      - place
      - authority
      - confidence
{%- endset -%}

{% set metadata_dict = fromyaml(yaml_metadata) %}

WITH staged AS (
    {{ automate_dv.stage(
        include_source_columns=true,
        source_model=metadata_dict['source_model'],
        derived_columns=metadata_dict['derived_columns'],
        null_columns=none,
        hashed_columns=metadata_dict['hashed_columns'],
        ranked_columns=none
    ) }}
)

SELECT *
FROM staged
WHERE resource_type_id = 2
   OR metadata_value_id = -1
