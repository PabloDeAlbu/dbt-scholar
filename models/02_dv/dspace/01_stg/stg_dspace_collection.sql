{%- set yaml_metadata -%}
source_model: "ldg_dspace_collection"
derived_columns:
  source: "!DSPACEDB"
  load_datetime: _load_datetime
  effective_from: _load_datetime
  start_date: _load_datetime
  end_date: to_date('9999-12-31', 'YYYY-MM-DD')
hashed_columns:
  collection_hk: collection_uuid
  submitter_hk: submitter
  collection_submitter_hk:
    - collection_uuid
    - submitter
  collection_hashdiff:
    is_hashdiff: true
    columns:
      - collection_uuid
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
