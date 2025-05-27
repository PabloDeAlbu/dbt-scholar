{{ config(materialized='incremental') }}

{%- set source_model = "stg_openaire_researchproduct_originalid" -%}
{%- set src_pk = "researchproduct_originalid_hk" -%}
{%- set src_fk = ["researchproduct_hk","original_hk"] -%}
{%- set src_ldts = "load_datetime" -%}
{%- set src_source = "source" -%}

{{ automate_dv.link(src_pk=src_pk, src_fk=src_fk, src_ldts=src_ldts,
                    src_source=src_source, source_model=source_model) }}
