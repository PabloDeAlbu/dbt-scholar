{%- set yaml_metadata -%}
source_model: "ldg_dspace_metadatafieldregistry"
derived_columns:
  source: "!DSPACEDB"
  load_datetime: load_datetime
  effective_from: load_datetime
  start_date: load_datetime
  end_date: to_date('9999-12-31', 'YYYY-MM-DD')
hashed_columns:
  metadatafield_hk: metadata_field_id
  metadataschema_hk: metadata_schema_id
  metadatafield_metadataschema_hk:
    - metadata_field_id
    - metadata_schema_id
  metadatafieldregistry_hashdiff:
    is_hashdiff: true
    columns:
      - metadata_field_id
      - metadata_schema_id
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
