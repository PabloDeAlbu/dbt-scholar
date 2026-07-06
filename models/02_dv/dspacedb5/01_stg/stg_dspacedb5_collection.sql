{{ config(materialized='view') }}

{%- set scope_relation = ref('ldg_dspacedb5__scope') | string | replace('"', '') -%}
{%- set scope_bk = "(SELECT institution_ror || '||' || source_label || '||' || base_url FROM " ~ scope_relation ~ ")" -%}
{%- set yaml_metadata -%}
source_model: ldg_dspacedb5_collection
derived_columns:
  scope_bk: "{{ scope_bk }}"
  collection_bk: "{{ scope_bk }} || '||' || collection_id::text"
  source_label: "(SELECT source_label FROM {{ scope_relation }})"
  institution_ror: "(SELECT institution_ror FROM {{ scope_relation }})"
  base_url: "(SELECT base_url FROM {{ scope_relation }})"
  source: "(SELECT source_label FROM {{ scope_relation }})"
  extract_datetime: "(SELECT extract_datetime FROM {{ scope_relation }})"
  load_datetime: "(SELECT load_datetime FROM {{ scope_relation }})"
  effective_from: "(SELECT extract_datetime FROM {{ scope_relation }})"
  start_date: "(SELECT extract_datetime FROM {{ scope_relation }})"
  end_date: "TO_DATE('9999-12-31', 'YYYY-MM-DD')"
hashed_columns:
  scope_hk: scope_bk
  collection_hk: collection_bk
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
