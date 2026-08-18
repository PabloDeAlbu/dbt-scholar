# dbt-scholar

English | [Español](LEEME.md)

A dbt project for organizing scholarly information sources and building
reusable models and analytical outputs.

## Layers

- `models/01_ldg`: minimal normalization of input data.
- `models/02_dv`: history tracking through Data Vault when required by the source.
- `models/03_dm`: shared dimensional models.
- `models/04_org`: facts, dimensions, and bridges specific to an organization or institutional source.
- `models/04_viz`: views intended for visualization.
- `models/05_analytics`: models and results for a specific analysis or project.

Physical schemas are generated according to the layer. For example:

- `models/04_org/unlp/sedicidb` is materialized in `dm_unlp_sedicidb`.
- `models/05_analytics/unlp/libros` is materialized in `analytics_unlp_libros`.

## Institutional sources

Local loading, modeling, and building of the SEDICI base are described in the
[SEDICI model guide](models/04_org/unlp/sedicidb/LEEME.md), currently in Spanish.

## Export a model to CSV

A model materialized in the local PostgreSQL database can be exported with:

```bash
make export MODEL=fct_unlp_sedicidb_metadatafield_usage
```

The file is generated in `var/exports/` with the current date:

```text
var/exports/fct_unlp_sedicidb_metadatafield_usage_YYYY-MM-DD.csv
```

The date can be overridden when the source snapshot needs to be identified:

```bash
make export \
  MODEL=fct_unlp_sedicidb_metadatafield_usage \
  DATE=2026-08-02
```

Files under `var/` are not versioned.

## More information

Modeling, execution, and contribution conventions are documented in
[CONTRIBUTING.md](CONTRIBUTING.md).
