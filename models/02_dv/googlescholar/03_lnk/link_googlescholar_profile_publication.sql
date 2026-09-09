{{ config(materialized='incremental') }}

{%- set source_model = "stg_googlescholar_profile_publication" -%}
{%- set src_pk = "profile_publication_hk" -%}
{%- set src_fk = ["profile_hk", "scholar_publication_id"] -%}
{%- set src_ldts = "load_datetime" -%}
{%- set src_source = "source" -%}

{{ automate_dv.link(
    src_pk=src_pk,
    src_fk=src_fk,
    src_ldts=src_ldts,
    src_source=src_source,
    source_model=source_model
) }}
