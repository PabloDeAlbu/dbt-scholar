SELECT ir.item_hk
FROM {{ ref('fct_unlp_ir_item_publication') }} AS ir
LEFT JOIN {{ ref('fct_unlp_publication') }} AS pub
    USING (item_hk)
WHERE pub.item_hk IS NULL
