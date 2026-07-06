{{ config(materialized='view') }}

{%- set yaml_metadata -%}
source_model: ldg_dspacedb5__scope
derived_columns:
  scope_bk: "institution_ror || '||' || source_label || '||' || base_url"
  source: source_label
  effective_from: "COALESCE(extract_datetime, load_datetime)"
  start_date: "COALESCE(extract_datetime, load_datetime)"
  end_date: "TO_DATE('9999-12-31', 'YYYY-MM-DD')"
hashed_columns:
  scope_hk: scope_bk
  extract_cdk:
    - scope_bk
    - extract_datetime
  extract_hashdiff:
    is_hashdiff: true
    columns:
      - scope_bk
      - institution_ror
      - source_label
      - base_url
      - extract_datetime
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
