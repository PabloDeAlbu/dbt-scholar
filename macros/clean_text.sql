{% macro clean_text(field) %}
COALESCE(
    TRIM(
        REGEXP_REPLACE(
            REPLACE(
                REPLACE(
                    REPLACE(
                        REPLACE({{ field }}::text, chr(8232), ' '),
                        chr(8233), ' '
                    ),
                    chr(160), ' '
                ),
                chr(65279), ''
            ),
            '\s+',
            ' ',
            'g'
        )
    ),
    ''
)
{% endmacro %}