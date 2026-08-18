# dbt-scholar

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

## Desarrollo local con SEDICI

El dump vigente de la base DSpace 5 se restaura directamente en el esquema
`ldg_sedicidb` de PostgreSQL. Esta fuente representa un estado completo del
repositorio y actualmente no pasa por Data Vault:

```text
ldg_sedicidb
    -> models/04_org/unlp/sedicidb
    -> dm_unlp_sedicidb
    -> models/05_analytics/unlp/*
```

La capa `04_org/unlp/sedicidb` busca mantener una base sencilla y trazable:

- los modelos `int_` preparan metadatafields concretos;
- las `dim_` representan entidades como comunidades, colecciones o personas;
- las `brg_` conservan relaciones multivaluadas entre entidades;
- las `fct_` declaran un hecho y un grano explícitos;
- `dedup/` adapta publicaciones al contrato requerido por procesos de deduplicación.

Para validar la conexión local:

```bash
dbt debug --target dev_docker
```

Para construir y probar solamente la base SEDICI, sin ejecutar tests de
consumidores analytics no seleccionados:

```bash
dbt build --target dev_docker --threads 1 \
  --selector unlp_sedicidb
```

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
