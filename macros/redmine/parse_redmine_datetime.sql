{% macro parse_redmine_datetime(expression) -%}
    CASE
        WHEN NULLIF(BTRIM({{ expression }}), '') IS NULL THEN NULL
        ELSE TO_TIMESTAMP(
            REPLACE(
                REPLACE(
                    REPLACE(
                        REPLACE(
                            REPLACE(
                                REPLACE(
                                    REPLACE(
                                        REPLACE(
                                            REPLACE(
                                                REPLACE(
                                                    REPLACE(
                                                        REPLACE(
                                                            {{ expression }},
                                                            'Enero', 'January'
                                                        ),
                                                        'Febrero', 'February'
                                                    ),
                                                    'Marzo', 'March'
                                                ),
                                                'Abril', 'April'
                                            ),
                                            'Mayo', 'May'
                                        ),
                                        'Junio', 'June'
                                    ),
                                    'Julio', 'July'
                                ),
                                'Agosto', 'August'
                            ),
                            'Septiembre', 'September'
                        ),
                        'Octubre', 'October'
                    ),
                    'Noviembre', 'November'
                ),
                'Diciembre', 'December'
            ),
            'DD Month YYYY HH12:MI AM'
        )::timestamp
    END
{%- endmacro %}
