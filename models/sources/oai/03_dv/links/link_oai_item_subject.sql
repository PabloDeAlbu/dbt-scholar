{{ config(materialized='incremental') }}

{%- set source_model = "stg_oai_item_subjects" -%}
{%- set src_pk = "item_subject_hk" -%}
{%- set src_fk = ["item_hk", "subject_hk"] -%}
{%- set src_ldts = "load_datetime" -%}
{%- set src_source = "source" -%}

{{ automate_dv.link(src_pk=src_pk, src_fk=src_fk, src_ldts=src_ldts,
                    src_source=src_source, source_model=source_model) }}
