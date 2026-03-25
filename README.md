# dbt-scholar

## Project layout

Models are organized by layer:

- `models/01_ldg`: landing
- `models/02_dv`: data vault
- `models/03_dm`: data marts
- `models/04_viz`: analytics views

## Schema naming

Final physical schemas are defined by both `dbt_project.yml` and `macros/generate_schema_name.sql`.
The macro adds a layer prefix:

- `models/01_ldg/*` -> `ldg_<schema>`
- `models/02_dv/*` -> `dv_<schema>`
- `models/03_dm/*` -> `dm_<schema>`
- `models/04_viz/*` -> `dm_<schema>`
- `seeds/*` -> `ldg_<schema>`

Examples:

- `models/03_dm/core/openalex/*` with `+schema: openalex` builds in `dm_openalex`
- `models/03_dm/core/openaire/*` with `+schema: openaire` builds in `dm_openaire`
- models in `models/03_dm/core/` root use `+schema: core` and build in `dm_core`

Notes:

- Subdirectories can override `+schema` from the parent directory.
- A model placed directly under `models/03_dm/core/` only builds in `dm_core` if `03_dm.core` sets `+schema: core` in `dbt_project.yml`.

## Common commands

- `dbt parse`
- `dbt run`
- `dbt test`
