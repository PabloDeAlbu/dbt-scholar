{{ config(materialized='view') }}

{%- set yaml_metadata -%}
source_model: "ldg_oai_sets"
derived_columns:
  source: _source_key
  repository_identifier: _repository_identifier
  institution_ror: _institution_ror
  base_url: _base_url
  context: _context
  partition_id: _partition_id
  load_datetime: _load_datetime
  effective_from: extract_datetime
hashed_columns:
  set_hk:
    - _repository_identifier
    - set_id
  set_hashdiff:
    is_hashdiff: true
    columns:
      - set_name
      - _institution_ror
      - _base_url
      - _context
{%- endset -%}

{% set metadata_dict = fromyaml(yaml_metadata) %}

{{ automate_dv.stage(
    include_source_columns=true,
    source_model=metadata_dict['source_model'],
    derived_columns=metadata_dict['derived_columns'],
    null_columns=none,
    hashed_columns=metadata_dict['hashed_columns'],
    ranked_columns=none
) }}
