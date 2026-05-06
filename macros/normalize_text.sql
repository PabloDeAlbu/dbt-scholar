{% macro normalize_text(field, ghost=true) %}
COALESCE(
    NULLIF(BTRIM({{ field }}::text), ''),
    {% if ghost %}'!UNKNOWN'{% else %}null{% endif %}
)
{% endmacro %}
