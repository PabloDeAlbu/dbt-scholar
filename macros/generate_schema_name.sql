{% macro generate_schema_name(custom_schema_name, node) -%}

  {%- set default_schema = target.schema -%}
  {%- set path = node.original_file_path -%}
  {%- set trimmed_name = custom_schema_name | trim if custom_schema_name else '' -%}

  {%- if path.startswith('models/sources/') -%}
    dv_{{ trimmed_name }}

  {%- elif path.startswith('models/data_mart/') and trimmed_name != '' -%}
    dm_{{ trimmed_name }}

  {%- else -%}
    {{ default_schema }}
  {%- endif -%}

{%- endmacro %}