{{ config(materialized='incremental') }}

{%- set source_model = "stg_dspace_metadatavalue" -%}
{%- set src_pk = "metadatavalue_dspaceobject_hk" -%}
{%- set src_fk = ["metadatavalue_hk", "dspaceobject_hk"] -%}
{%- set src_ldts = "load_datetime" -%}
{%- set src_source = "source" -%}

{{ automate_dv.link(src_pk=src_pk, src_fk=src_fk, src_ldts=src_ldts,
                    src_source=src_source, source_model=source_model) }}
