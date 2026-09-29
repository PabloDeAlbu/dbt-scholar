# Digitalización de materiales en SEDICI

Este caso analítico parte de la estructura de los issues del proyecto de
digitalización en Redmine. Su primera versión describe la demanda registrada y
el equipamiento asignado. No evalúa todavía el funcionamiento ni el desempeño
del DAL.

## Preguntas iniciales

1. ¿Qué tipos de recursos fueron registrados para digitalizar?
2. ¿Qué equipamiento fue asignado a esos recursos?
3. ¿Qué restricciones de manipulación aparecen para cada equipamiento?
4. ¿Qué tipos de recursos fueron asignados al DAL?
5. ¿Qué instituciones acercaron materiales y qué equipamiento requieren?

## Fuente y corte

El seed `seed_unlp_sedici_dal_redmine_issue_example` contiene solamente casos
ficticios que reproducen la estructura observada en la exportación del **28 de
septiembre de 2026**. Los nombres técnicos de sus columnas fueron
normalizados, pero cada descripción del `schema.yml` registra el encabezado
original de Redmine.

La muestra permite validar recursos con y sin URL publicada y tareas con una
relación jerárquica. No reproduce cantidades, distribuciones ni datos
operativos del proyecto real.

## Modelos

- `unlp_sedici_dal_recurso_base`: una fila por issue y fecha de extracción, con
  tipos relacionales y trazabilidad hacia los campos originales.
- `unlp_sedici_dal_issue_publicacion`: correspondencia exacta entre la URL
  publicada en Redmine y el handle visible en SEDICI.
- `unlp_sedici_dal_publicacion_digitalizacion`: evidencias independientes de
  digitalización y casos que requieren revisión.
- `unlp_sedici_dal_dashboard`: nombres y estados preparados para la lectura
  en una herramienta de visualización.

## Alcance y límites

El escáner informado representa una asignación registrada en Redmine. No
demuestra que el recurso haya sido procesado efectivamente con ese equipo.

Los datos no permiten todavía medir dimensiones físicas, cantidad de páginas,
peso, resolución, fidelidad, errores, tiempo de procesamiento ni esfuerzo del
operador. Por lo tanto, no deben utilizarse para comparar productividad o
calidad entre equipos.

La exportación completa permanece fuera de Git. Una futura integración deberá
obtenerla desde Redmine y cargarla en landing antes de reemplazar este seed.

## Ejecución local

```bash
dbt seed --target dev_docker --select seed_unlp_sedici_dal_redmine_issue_example
dbt build --target dev_docker --select path:models/05_analytics/unlp/sedici/dal
```
