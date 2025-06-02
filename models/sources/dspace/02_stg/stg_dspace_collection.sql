{%- set yaml_metadata -%}
source_model: "ldg_dspace_collection"
derived_columns:
  source: "!DSPACEDB"
  load_datetime: load_datetime
  effective_from: load_datetime
  start_date: load_datetime
  end_date: to_date('9999-12-31', 'YYYY-MM-DD')
hashed_columns:
  collection_hk: collection_id
  uuid_hk: uuid
  submitter_hk: submitter
  uuid_submitter_hk:
    - uuid
    - submitter
  collection_hashdiff:
    is_hashdiff: true
    columns:
      - collection_id
      - uuid
      - submitter
      - template_item_id
      - logo_bitstream_id
      - admin
{%- endset -%}

{% set metadata_dict = fromyaml(yaml_metadata) %}

{{ automate_dv.stage(include_source_columns=true,
                     source_model=metadata_dict['source_model'],
                     derived_columns=metadata_dict['derived_columns'],
                     null_columns=none,
                     hashed_columns=metadata_dict['hashed_columns'],
                     ranked_columns=none) }}
