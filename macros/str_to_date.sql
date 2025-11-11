{% macro str_to_date(str_date) %}
(CASE 
    -- Se maneja  el formato con '/', ej: '2018/12/05' o '2018/12'
    WHEN ({{str_date}} ~ '^\d{4}/\d{1,2}/\d{1,2}$') 
        AND (CAST(SUBSTRING(REPLACE({{str_date}}, '/', '-'), 6, 2) AS INTEGER) BETWEEN 1 AND 12)
        AND (CAST(SUBSTRING(REPLACE({{str_date}}, '/', '-'), 9, 2) AS INTEGER) BETWEEN 1 AND 31)
    THEN REPLACE({{str_date}}, '/', '-')
    
    WHEN ({{str_date}} ~ '^\d{4}/\d{1,2}$') 
        AND (CAST(SUBSTRING(REPLACE({{str_date}}, '/', '-'), 6, 2) AS INTEGER) BETWEEN 1 AND 12)
    THEN CONCAT(REPLACE({{str_date}}, '/', '-'), '-01')

    -- Se maneja  el formato 'YYYY-' o 'YYYY'
    WHEN ({{str_date}} ~ '^\d{4}-$') THEN CONCAT({{str_date}}, '01-01')
    WHEN ({{str_date}} ~ '^\d{4}$') THEN CONCAT({{str_date}}, '-01-01')

    -- Se maneja  el formato 'YYYY-MM'
    WHEN ({{str_date}} ~ '^\d{4}-\d{1,2}$')
        AND (CAST(SUBSTRING({{str_date}}, 6, 2) AS INTEGER) BETWEEN 1 AND 12)
    THEN CONCAT({{str_date}}, '-01')

    -- Se maneja  el caso de 'YYYY seguido de texto', ej: '1906 TOMO II'
    WHEN ({{str_date}} ~ '^\d{4}\s.+$') THEN CONCAT(SUBSTRING({{str_date}}, 1, 4), '-01-01')

    -- Se Valida formato general de fecha (YYYY-MM-DD)
    WHEN ({{str_date}} ~ '^\d{4}-\d{1,2}-\d{1,2}$') 
         AND (CAST(SUBSTRING({{str_date}}, 6, 2) AS INTEGER) BETWEEN 1 AND 12) -- Mes válido
         AND (CAST(SUBSTRING({{str_date}}, 9, 2) AS INTEGER) BETWEEN 1 AND 31) -- Día válido
    THEN {{str_date}}

    -- Se maneja formato ISO con componente horaria, ej: '2019-10-30T11:38:41Z'
    WHEN ({{str_date}} ~ '^\d{4}-\d{1,2}-\d{1,2}T.*$')
         AND (CAST(SUBSTRING({{str_date}}, 6, 2) AS INTEGER) BETWEEN 1 AND 12)
         AND (CAST(SUBSTRING({{str_date}}, 9, 2) AS INTEGER) BETWEEN 1 AND 31)
    THEN SUBSTRING({{str_date}}, 1, 10)

    -- Se devuelve una "fecha de error" si el formato es incorrecto o tiene valores inválidos
    ELSE '9999-12-31'
END)::date
{% endmacro %}
