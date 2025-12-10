{{ config(materialized='incremental') }}

{%- set source_model = "stg_oai_record_rights" -%}
{%- set src_pk = "record_right_hk" -%}
{%- set src_fk = ["record_hk", "dc_right_hk"] -%}
{%- set src_ldts = "load_datetime" -%}
{%- set src_source = "source" -%}

{{ automate_dv.link(src_pk=src_pk, src_fk=src_fk, src_ldts=src_ldts,
                    src_source=src_source, source_model=source_model) }}
