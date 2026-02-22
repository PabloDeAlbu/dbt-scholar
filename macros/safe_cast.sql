{% macro safe_cast(relation, col_name, data_type=None, alias=None, default_sql='null', col_names=None, default_mode='null') %}
  {# relation: ref()/source()
     col_name: nombre EXACTO en la tabla
     data_type: 'text', 'int', 'boolean', 'timestamp', etc. (si es none, no castea)
     alias: nombre final en el modelo
     default_sql: qué devolver si no existe (por defecto null)
     col_names: lista opcional de columnas (para evitar múltiples lookups)
  #}

  {% set out_alias = alias if alias is not none else (col_name | replace('.', '_')) %}

  {% set resolved_default_sql = default_sql %}
  {% if (default_mode | lower) == 'ghost' and default_sql == 'null' and data_type is not none %}
    {% set dt = (data_type | lower) %}
    {% if dt in ['text', 'string', 'varchar', 'character varying'] %}
      {% set resolved_default_sql = "'!UNKNOWN'" %}
    {% elif dt in ['bool', 'boolean'] %}
      {% set resolved_default_sql = 'false' %}
    {% elif dt in ['int', 'integer', 'bigint', 'smallint'] %}
      {% set resolved_default_sql = '-1' %}
    {% elif dt in ['float', 'real', 'double', 'double precision', 'numeric', 'decimal'] %}
      {% set resolved_default_sql = '-1' %}
    {% elif dt in ['timestamp', 'timestamptz', 'timestamp with time zone', 'timestamp without time zone', 'date'] %}
      {% set resolved_default_sql = "'1900-01-01'" %}
    {% endif %}
  {% endif %}

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
        ({{ resolved_default_sql }}) as {{ out_alias }}
      {% else %}
        ({{ resolved_default_sql }})::{{ data_type }} as {{ out_alias }}
      {% endif %}
    {% endif %}
  {% endif %}
{% endmacro %}
