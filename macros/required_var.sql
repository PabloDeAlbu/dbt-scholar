{% macro required_var(var_name) -%}
    {%- set value = var(var_name, '') -%}
    {%- set normalized = value | string | trim | upper -%}
    {%- if execute and (
        value is none
        or (value is string and value | trim == '')
        or normalized in ['REPLACEME', 'REPLACE ME']
    ) -%}
        {{ exceptions.raise_compiler_error(
            "Required dbt var '" ~ var_name ~ "' is not configured. " ~
            "Set it in dbt_project.yml or pass it with --vars."
        ) }}
    {%- endif -%}
    {{ return(value) }}
{%- endmacro %}
