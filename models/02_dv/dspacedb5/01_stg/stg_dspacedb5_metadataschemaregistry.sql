{{ config(materialized='view') }}

{%- set yaml_metadata -%}
source_model: ldg_dspacedb5_metadataschemaregistry
derived_columns:
  scope_bk: "institution_ror || '||' || source_label || '||' || base_url"
  metadataschema_bk: "institution_ror || '||' || source_label || '||' || base_url || '||' || metadata_schema_id::text"
  source: source_label
  effective_from: extract_datetime
  start_date: extract_datetime
  end_date: "TO_DATE('9999-12-31', 'YYYY-MM-DD')"
hashed_columns:
  scope_hk: scope_bk
  metadataschema_hk: metadataschema_bk
  metadataschemaregistry_hashdiff:
    is_hashdiff: true
    columns:
      - metadataschema_bk
      - namespace
      - short_id
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
