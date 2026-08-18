# Modelos de SEDICI

Esta carpeta contiene la base organizacional construida a partir de la base de
datos de SEDICI.

El dump vigente de DSpace 5 se restaura directamente en el esquema
`ldg_sedicidb` de PostgreSQL. La fuente representa un estado completo del
repositorio y actualmente no pasa por Data Vault:

```text
ldg_sedicidb
    -> models/04_org/unlp/sedicidb
    -> dm_unlp_sedicidb
    -> models/05_analytics/unlp/*
```

Esta capa busca mantener una base sencilla y trazable:

- los modelos `int_` preparan metadatafields concretos;
- las `dim_` representan entidades como comunidades, colecciones o personas;
- las `brg_` conservan relaciones multivaluadas entre entidades;
- las `fct_` declaran un hecho y un grano explícitos;
- `dedup/` adapta publicaciones al contrato requerido por procesos de deduplicación.

## Desarrollo local

Para validar la conexión local:

```bash
dbt debug --target dev_docker
```

Para construir y probar solamente esta base, sin ejecutar tests de consumidores
analytics no seleccionados:

```bash
dbt build --target dev_docker --threads 1 \
  --selector unlp_sedicidb
```
