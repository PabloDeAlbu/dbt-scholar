{{ config(materialized='incremental') }}

{%- set yaml_metadata -%}
source_model: "stg_openaire_researchproduct"
src_pk: "researchproduct_hk"
src_hashdiff:
  source_column: "researchproduct_hashdiff"
  alias: "hashdiff"
src_payload:
  - publicly_funded
  - main_title
  - publication_date
  - is_green
  - is_in_diamond_journal
  - language_code
  - language_label
  - best_access_right
  - best_access_right_uri
  - citation_class
  - citation_count
  - impulse
  - impulse_class
  - influence
  - influence_class
  - popularity
  - popularity_class
  - downloads
  - views
  - publisher
  - embargo_end_date
  - load_datetime
src_eff: "load_datetime"
src_ldts: "load_datetime"
src_source: "source"
{%- endset -%}

{% set metadata_dict = fromyaml(yaml_metadata) %}

{{ automate_dv.sat(src_pk=metadata_dict["src_pk"],
                   src_hashdiff=metadata_dict["src_hashdiff"],
                   src_payload=metadata_dict["src_payload"],
                   src_eff=metadata_dict["src_eff"],
                   src_ldts=metadata_dict["src_ldts"],
                   src_source=metadata_dict["src_source"],
                   source_model=metadata_dict["source_model"])   }}
