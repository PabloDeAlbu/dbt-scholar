# Issue 14365

Modelos e insumos utilizados para preparar, conciliar y exportar las
deduplicaciones de libros y tesis entre Koha FDA y SEDICI.

Los modelos se materializan en el esquema
`analytics_unlp_sedici_issue_14365`. Sus nombres siguen el patrón
`unlp_sedici_<etapa>_<acción>_<descripción>` para que el orden alfabético
represente el orden de trabajo:

- `01`: dedup de libros Koha FDA → SEDICI.
- `02`: dedup de libros SEDICI FDA → Koha y filtro de candidatos sin match.
- `03`: dedup de tesis Koha FDA → SEDICI, revisión y CSV de actualización.
- `04`: dedup de tesis sin SEDICI → planilla de digitalización y exports.

## Convenciones

- Los modelos `dedup_*_input_1` y `dedup_*_input_2` producen los archivos de
  entrada para el deduplicador.
- Los modelos `base` consolidan resultados y decisiones de conciliación.
- Los modelos `filter` exponen subconjuntos orientados a una revisión o acción
  concreta. Pueden materializarse como vistas cuando no necesitan duplicar los
  datos de la base.
- Los modelos `export` preparan conjuntos destinados a consumirse fuera del
  warehouse.

Los resultados externos y las decisiones manuales utilizados por los modelos
se conservan en `seeds/unlp/sedici/14365`.

## Ejecución

```bash
dbt build \
  --select path:models/05_analytics/unlp/sedici/issue_14365 \
  --target dw_sedici
```

El número de inventario de Koha debe conservarse para trazabilidad, aunque no
forme parte de los metadatos disponibles en SEDICI.
