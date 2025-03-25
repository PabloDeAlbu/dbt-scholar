{{ config(materialized='incremental') }}

{%- set source_model = "stg_coar_openaire" -%}
{%- set src_pk = "link_coar_openaire_hk" -%}
{%- set src_fk = ["coar_hk", "type_hk"] -%}
{%- set src_ldts = "load_datetime" -%}
{%- set src_source = "source" -%}

{{ automate_dv.link(src_pk=src_pk, src_fk=src_fk, src_ldts=src_ldts,
                    src_source=src_source, source_model=source_model) }}
