{{ config(materialized='view') }}

{%- set yaml_metadata -%}
source_model: "ldg_dspacedb5_metadataschemaregistry"
derived_columns:
  source: _source_label
  load_datetime: _load_datetime
  effective_from: _extract_datetime
  start_date: _extract_datetime
  end_date: to_date('9999-12-31', 'YYYY-MM-DD')
hashed_columns:
  metadataschema_hk: metadataschema_bk
  metadataschemaregistry_hashdiff:
    is_hashdiff: true
    columns:
      - metadataschema_bk
      - namespace
      - short_id
{%- endset -%}

{% set metadata_dict = fromyaml(yaml_metadata) %}

{{ automate_dv.stage(include_source_columns=true,
                     source_model=metadata_dict['source_model'],
                     derived_columns=metadata_dict['derived_columns'],
                     null_columns=none,
                     hashed_columns=metadata_dict['hashed_columns'],
                     ranked_columns=none) }}
