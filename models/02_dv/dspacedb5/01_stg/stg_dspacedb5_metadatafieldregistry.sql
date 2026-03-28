{{ config(materialized='view') }}

{%- set yaml_metadata -%}
source_model: "ldg_dspacedb5_metadatafieldregistry"
derived_columns:
  source: _source_label
  load_datetime: _load_datetime
  effective_from: _extract_datetime
  start_date: _extract_datetime
  end_date: to_date('9999-12-31', 'YYYY-MM-DD')
hashed_columns:
  metadatafield_hk: metadatafield_bk
  metadataschema_hk: metadataschema_bk
  metadatafield_metadataschema_hk:
    - metadatafield_bk
    - metadataschema_bk
  metadatafieldregistry_hashdiff:
    is_hashdiff: true
    columns:
      - metadatafield_bk
      - metadataschema_bk
      - element
      - qualifier
      - scope_note
{%- endset -%}

{% set metadata_dict = fromyaml(yaml_metadata) %}

{{ automate_dv.stage(include_source_columns=true,
                     source_model=metadata_dict['source_model'],
                     derived_columns=metadata_dict['derived_columns'],
                     null_columns=none,
                     hashed_columns=metadata_dict['hashed_columns'],
                     ranked_columns=none) }}
