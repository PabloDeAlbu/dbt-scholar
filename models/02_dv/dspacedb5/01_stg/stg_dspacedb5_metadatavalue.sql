{{ config(materialized='view') }}

{%- set yaml_metadata -%}
source_model: "ldg_dspacedb5_metadatavalue"
derived_columns:
  source: _source_label
  load_datetime: _load_datetime
  effective_from: _extract_datetime
  start_date: _extract_datetime
  end_date: to_date('9999-12-31', 'YYYY-MM-DD')
hashed_columns:
  authority_hk: authority
  metadatavalue_hk: metadatavalue_bk
  metadatafield_hk: metadatafield_bk
  resource_hk: resource_bk
  metadatavalue_authority_hk:
    - metadatavalue_bk
    - authority
  metadatavalue_resource_hk:
    - metadatavalue_bk
    - resource_bk
  metadatavalue_resourcetype_hk:
    - metadatavalue_bk
    - resource_type_id
  metadatavalue_metadatafield_hk:
    - metadatavalue_bk
    - metadatafield_bk
  metadatavalue_hashdiff:
    is_hashdiff: true
    columns:
      - metadatavalue_bk
      - resource_bk
      - resource_type_id
      - metadatafield_bk
      - text_value
      - text_lang
      - place
      - authority
      - confidence
{%- endset -%}

{% set metadata_dict = fromyaml(yaml_metadata) %}

{{ automate_dv.stage(include_source_columns=true,
                     source_model=metadata_dict['source_model'],
                     derived_columns=metadata_dict['derived_columns'],
                     null_columns=none,
                     hashed_columns=metadata_dict['hashed_columns'],
                     ranked_columns=none) }}
