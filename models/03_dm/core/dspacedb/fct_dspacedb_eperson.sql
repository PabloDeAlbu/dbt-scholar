{{ config(materialized='table') }}

-- FIXME: este hecho se construye directo sobre `ldg/source` porque la rama `dspacedb`
-- todavía no tiene un camino DV completo para eperson, grupos, policies y workflow.
WITH base_eperson AS (
    SELECT
        eperson.eperson_id,
        eperson.uuid AS eperson_uuid,
        eperson.email,
        eperson.netid,
        eperson.can_log_in,
        eperson.require_certificate,
        eperson.self_registered,
        eperson.last_active
    FROM {{ source('dspacedb', 'eperson') }} AS eperson
),

effective_group_membership AS (
    SELECT
        eperson_id AS eperson_uuid,
        eperson_group_id AS group_uuid
    FROM {{ source('dspacedb', 'epersongroup2eperson') }}

    UNION

    SELECT
        e2e.eperson_id AS eperson_uuid,
        g2gc.parent_id AS group_uuid
    FROM {{ source('dspacedb', 'epersongroup2eperson') }} AS e2e
    JOIN {{ source('dspacedb', 'group2groupcache') }} AS g2gc
        ON g2gc.child_id = e2e.eperson_group_id
),

effective_groups AS (
    SELECT DISTINCT
        membership.eperson_uuid,
        membership.group_uuid,
        grp.name AS group_name
    FROM effective_group_membership AS membership
    JOIN {{ source('dspacedb', 'epersongroup') }} AS grp
        ON grp.uuid = membership.group_uuid
),

group_summary AS (
    SELECT
        eperson_uuid,
        COUNT(DISTINCT group_uuid) AS group_count
    FROM effective_groups
    GROUP BY eperson_uuid
),

administrator_group AS (
    SELECT
        eg.eperson_uuid,
        TRUE AS administrator_group_member_flag
    FROM effective_groups AS eg
    WHERE LOWER(eg.group_name) = 'administrator'
    GROUP BY eg.eperson_uuid
),

collection_admin_group AS (
    SELECT
        eg.eperson_uuid,
        TRUE AS collection_admin_group_member_flag
    FROM effective_groups AS eg
    JOIN {{ source('dspacedb', 'collection') }} AS col
        ON col.admin = eg.group_uuid
    GROUP BY eg.eperson_uuid
),

community_admin_group AS (
    SELECT
        eg.eperson_uuid,
        TRUE AS community_admin_group_member_flag
    FROM effective_groups AS eg
    JOIN {{ source('dspacedb', 'community') }} AS comm
        ON comm.admin = eg.group_uuid
    GROUP BY eg.eperson_uuid
),

administrator_summary AS (
    SELECT
        base.eperson_uuid,
        (
            COALESCE(admin.administrator_group_member_flag, FALSE)
            OR COALESCE(col_admin.collection_admin_group_member_flag, FALSE)
            OR COALESCE(comm_admin.community_admin_group_member_flag, FALSE)
        ) AS administrator_flag
    FROM base_eperson AS base
    LEFT JOIN administrator_group AS admin
        USING (eperson_uuid)
    LEFT JOIN collection_admin_group AS col_admin
        USING (eperson_uuid)
    LEFT JOIN community_admin_group AS comm_admin
        USING (eperson_uuid)
),
submitter_activity AS (
    SELECT
        submitter_id AS eperson_uuid,
        COUNT(*) AS submitted_item_count,
        MIN(last_modified) AS first_submitted_item_last_modified,
        MAX(last_modified) AS last_submitted_item_last_modified
    FROM {{ source('dspacedb', 'item') }}
    WHERE submitter_id IS NOT NULL
    GROUP BY submitter_id
),

final AS (
    SELECT
        eperson.eperson_id,
        eperson.eperson_uuid,
        eperson.email,
        eperson.netid,
        eperson.can_log_in,
        eperson.require_certificate,
        eperson.self_registered,
        eperson.last_active,
        grp.group_count,
        admin.administrator_flag AS is_administrator,
        submitter.submitted_item_count,
        submitter.first_submitted_item_last_modified,
        submitter.last_submitted_item_last_modified
    FROM base_eperson AS eperson
    LEFT JOIN group_summary AS grp
        USING (eperson_uuid)
    LEFT JOIN administrator_summary AS admin
        USING (eperson_uuid)
    LEFT JOIN submitter_activity AS submitter
        USING (eperson_uuid)
)

SELECT * FROM final
