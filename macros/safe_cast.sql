{% macro safe_cast(relation, col_name, data_type=None, alias=None, default_sql='null', col_names=None) %}
  {# relation: ref()/source()
     col_name: nombre EXACTO en la tabla
     data_type: 'text', 'int', 'boolean', 'timestamp', etc. (si es none, no castea)
     alias: nombre final en el modelo
     default_sql: qué devolver si no existe (por defecto null)
     col_names: lista opcional de columnas (para evitar múltiples lookups)
  #}

  {% set out_alias = alias if alias is not none else (col_name | replace('.', '_')) %}

  {# En parse/compile sin conexión (execute == false), asumimos que existe. #}
  {% if not execute %}
    {% if data_type is none %}
      {{ adapter.quote(col_name) }} as {{ out_alias }}
    {% else %}
      {{ adapter.quote(col_name) }}::{{ data_type }} as {{ out_alias }}
    {% endif %}
  {% else %}
    {% if col_names is none %}
      {% set cols = adapter.get_columns_in_relation(relation) %}
      {% set col_names = cols | map(attribute='name') | map('lower') | list %}
    {% else %}
      {% set col_names = col_names | map('lower') | list %}
    {% endif %}

    {% if (col_name | lower) in col_names %}
      {% if data_type is none %}
        {{ adapter.quote(col_name) }} as {{ out_alias }}
      {% else %}
        {{ adapter.quote(col_name) }}::{{ data_type }} as {{ out_alias }}
      {% endif %}
    {% else %}
      {% if data_type is none %}
        ({{ default_sql }}) as {{ out_alias }}
      {% else %}
        ({{ default_sql }})::{{ data_type }} as {{ out_alias }}
      {% endif %}
    {% endif %}
  {% endif %}
{% endmacro %}
