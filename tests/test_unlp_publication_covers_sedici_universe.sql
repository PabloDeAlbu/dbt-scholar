SELECT ir.item_hk
FROM {{ ref('fct_unlp_sedici_item_publication') }} AS ir
LEFT JOIN {{ ref('fct_unlp_publication') }} AS pub
    USING (item_hk)
WHERE pub.item_hk IS NULL
