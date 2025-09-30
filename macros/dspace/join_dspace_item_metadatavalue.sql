{% macro join_dspace_item_metadatavalue(fieldname, agg=true, filter=None, count=false, has_value=false) %}

    SELECT
        bridge_i_mv.item_hk,
        {% if agg %}
            STRING_AGG(mv.text_value, '|' ORDER BY mv.text_value) as text_value
        {% else %}
            mv.text_value
        {% endif %}
        {% if count %}
            , COUNT(DISTINCT NULLIF(TRIM(mv.text_value), ''))::int AS value_count
        {% endif %}
        {% if has_value %}
            , CASE WHEN (COUNT(*) > 0) THEN True ELSE FALSE END AS has_value
        {% endif %}
    FROM {{ ref('brg_dspace_item_metadatavalue') }} bridge_i_mv
    INNER JOIN {{ ref('dim_dspace_metadatavalue') }} mv USING (metadatavalue_hk)
    WHERE bridge_i_mv.metadatafield_fullname = '{{ fieldname }}'
    {% if filter %}
        AND {{ filter }}
    {% endif %}
    {% if agg or count %}
        GROUP BY bridge_i_mv.item_hk
    {% endif %}

{% endmacro %}
