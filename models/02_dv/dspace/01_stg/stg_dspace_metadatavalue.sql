{%- set yaml_metadata -%}
source_model: "ldg_dspace_metadatavalue"
derived_columns:
  source: "!DSPACEDB"
  load_datetime: dv_load_datetime
  effective_from: dv_load_datetime
  start_date: dv_load_datetime
  end_date: to_date('9999-12-31', 'YYYY-MM-DD')
hashed_columns:
  authority_hk: authority
  metadatavalue_hk: metadata_value_id
  metadatafield_hk: metadata_field_id
  dspaceobject_hk: dspace_object_id
  metadatavalue_dspaceobject_hk:
    - metadata_value_id
    - dspace_object_id
  metadatavalue_metadatafield_hk:
    - metadata_value_id
    - metadata_field_id
  metadatavalue_hashdiff:
    is_hashdiff: true
    columns:
      - metadata_value_id
      - metadata_field_id
      - text_value
      - text_lang
      - place
      - authority
      - confidence
      - dspace_object_id

{%- endset -%}

{% set metadata_dict = fromyaml(yaml_metadata) %}

{{ automate_dv.stage(include_source_columns=true,
                     source_model=metadata_dict['source_model'],
                     derived_columns=metadata_dict['derived_columns'],
                     null_columns=none,
                     hashed_columns=metadata_dict['hashed_columns'],
                     ranked_columns=none) }}
