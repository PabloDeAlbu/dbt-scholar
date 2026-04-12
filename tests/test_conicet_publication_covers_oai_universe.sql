SELECT oai.record_hk
FROM {{ ref('fct_conicet_oai_record_publication') }} AS oai
LEFT JOIN {{ ref('fct_conicet_publication') }} AS pub
    USING (record_hk)
WHERE pub.record_hk IS NULL
