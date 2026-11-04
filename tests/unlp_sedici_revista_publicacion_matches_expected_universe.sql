WITH expected AS (
    SELECT publication.item_id
    FROM {{ ref('unlp_sedici_publicacion_base') }} AS publication
    INNER JOIN {{ ref('dim_unlp_sedici_revista') }} AS revista
        ON revista.sedici_community_id = publication.owning_root_child_community_id
    WHERE publication.handle IS NOT NULL
      AND publication.owning_root_community_title = 'Revistas'
),

actual AS (
    SELECT item_id
    FROM {{ ref('fct_unlp_sedici_revista_publicacion') }}
)

SELECT
    expected.item_id AS expected_item_id,
    actual.item_id AS actual_item_id
FROM expected
FULL OUTER JOIN actual
    USING (item_id)
WHERE expected.item_id IS NULL
   OR actual.item_id IS NULL
