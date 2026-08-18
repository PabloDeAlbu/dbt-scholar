# dbt-scholar

[English](README.md) | Español

Proyecto dbt para organizar fuentes de información académica y construir
modelos reutilizables y salidas analíticas.

## Capas

- `models/01_ldg`: normalización mínima de datos de entrada.
- `models/02_dv`: historización mediante Data Vault cuando la fuente lo requiere.
- `models/03_dm`: modelos dimensionales compartidos.
- `models/04_org`: hechos, dimensiones y bridges propios de una organización o fuente institucional.
- `models/04_viz`: vistas orientadas a visualización.
- `models/05_analytics`: modelos y resultados de un análisis o proyecto específico.

Los esquemas físicos son generados según la capa. Por ejemplo:

- `models/04_org/unlp/sedicidb` se materializa en `dm_unlp_sedicidb`.
- `models/05_analytics/unlp/libros` se materializa en `analytics_unlp_libros`.

## Fuentes institucionales

La carga local, el modelado y la construcción de la base de SEDICI se describen
en la [guía de los modelos de SEDICI](models/04_org/unlp/sedicidb/LEEME.md).

## Exportar un modelo a CSV

Un modelo materializado en el PostgreSQL local puede exportarse mediante:

```bash
make export MODEL=fct_unlp_sedicidb_metadatafield_usage
```

El archivo se genera en `var/exports/` con la fecha actual:

```text
var/exports/fct_unlp_sedicidb_metadatafield_usage_YYYY-MM-DD.csv
```

La fecha puede sobrescribirse cuando se necesita identificar el snapshot de
la fuente:

```bash
make export \
  MODEL=fct_unlp_sedicidb_metadatafield_usage \
  DATE=2026-08-02
```

Los archivos de `var/` no se versionan.

## Más información

Las convenciones de modelado, ejecución y contribución están documentadas en
[CONTRIBUTING.md](CONTRIBUTING.md).
