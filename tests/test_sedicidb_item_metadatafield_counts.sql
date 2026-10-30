WITH expected AS (
    SELECT
        item_id,
        metadatafield_fullname,
        COUNT(*)::int AS observation_count,
        COUNT(text_value)::int AS value_count,
        COUNT(DISTINCT text_value)::int AS distinct_value_count
    FROM {{ ref('int_unlp_sedicidb_item_metadatavalue') }}
    GROUP BY
        item_id,
        metadatafield_fullname
),

actual AS (
    SELECT
        item_id,
        metadatafield_fullname,
        observation_count,
        value_count,
        distinct_value_count
    FROM {{ ref('int_unlp_sedicidb_item_metadatafield') }}
)

SELECT
    COALESCE(expected.item_id, actual.item_id) AS item_id,
    COALESCE(expected.metadatafield_fullname, actual.metadatafield_fullname)
        AS metadatafield_fullname
FROM expected
FULL OUTER JOIN actual
    USING (item_id, metadatafield_fullname)
WHERE expected.item_id IS NULL
    OR actual.item_id IS NULL
    OR expected.observation_count <> actual.observation_count
    OR expected.value_count <> actual.value_count
    OR expected.distinct_value_count <> actual.distinct_value_count
