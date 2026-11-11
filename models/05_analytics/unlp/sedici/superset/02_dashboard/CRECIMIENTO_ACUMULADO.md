# Crecimiento de las publicaciones en SEDICI

## Qué queremos mostrar

El crecimiento del repositorio puede observarse de dos maneras complementarias:

1. **Publicaciones ingresadas durante cada período:** muestra cuántas publicaciones se incorporaron en un año o mes determinado.
2. **Publicaciones acumuladas:** muestra cuántas publicaciones había alcanzado el repositorio hasta cada año o mes.

No son la misma métrica. La primera representa un flujo de nuevos ingresos. La segunda representa el total alcanzado a lo largo del tiempo.

Por ejemplo:

| Año | Ingresadas durante el año | Acumuladas hasta ese año |
| --: | -------------------------: | -----------------------: |
| 2023 | 100 | 100 |
| 2024 | 120 | 220 |
| 2025 | 80 | 300 |

Aunque en 2025 ingresaron menos publicaciones que en 2024, el total acumulado continuó creciendo.

## Fecha utilizada

El eje temporal se construye con `fecha_ingreso_sedici`, derivada del metadato `dc.date.accessioned`.

Esta fecha representa el ingreso del recurso al repositorio. El conjunto de datos también conserva `fecha_disponibilidad_sedici`, derivada de `dc.date.available`. Actualmente ambas pueden coincidir en la mayoría de los registros, pero representan conceptos diferentes y se mantienen separadas.

Para agrupar los datos están disponibles:

- `fecha_ingreso_sedici`: fecha completa de ingreso;
- `mes_ingreso_sedici`: año y mes, representados mediante el primer día del mes;
- `anio_ingreso_sedici`: año de ingreso.

## Cómo construir los gráficos en Superset

### Publicaciones ingresadas por año

Este gráfico muestra los nuevos ingresos de cada año.

- Visualización: **Bar Chart**.
- Columna temporal: `fecha_ingreso_sedici`.
- Granularidad temporal: **Year**.
- Rango temporal: **No filter**.
- Métrica: `COUNT(*)` o `COUNT_DISTINCT(handle)`.
- Rolling function: ninguna.

Título sugerido: **SEDICI · Publicaciones ingresadas por año**.

### Crecimiento acumulado por año

Este gráfico parte del conteo anual y suma cada resultado a los años anteriores.

- Visualización: **Area Chart**, **Line Chart** o **Bar Chart**.
- Columna temporal: `fecha_ingreso_sedici`.
- Granularidad temporal: **Year**.
- Rango temporal: **No filter**.
- Métrica: `COUNT(*)` o `COUNT_DISTINCT(handle)`.
- En **Advanced analytics → Rolling window**:
  - Rolling function: `cumsum`;
  - Periods: vacío;
  - Min periods: vacío.

Título sugerido: **SEDICI · Crecimiento acumulado de publicaciones**.

## Diferencia entre `sum` y `cumsum`

En Superset, `sum` y `cumsum` cumplen funciones distintas:

- `sum` calcula una suma móvil sobre una cantidad determinada de períodos. Por eso requiere indicar `Periods`.
- `cumsum` calcula una suma acumulada desde el primer período visible. No requiere indicar `Periods`.

Para representar el crecimiento histórico debe utilizarse `cumsum`.

## Criterio visual

Las barras funcionan bien para comparar cuántas publicaciones ingresaron en cada año. Una línea o un área resulta más clara para mostrar el crecimiento acumulado, porque comunica la evolución de un total que se construye a través del tiempo.

Los dos gráficos responden preguntas diferentes:

- **Barras:** ¿cuántas publicaciones ingresaron durante cada año?
- **Línea o área acumulada:** ¿cuántas publicaciones había alcanzado SEDICI hasta cada año?

Conviene presentarlos juntos cuando se quiera distinguir el ritmo anual de incorporación del tamaño alcanzado por el repositorio.

## Alcance del resultado

El conjunto de datos incluye publicaciones que actualmente están archivadas, visibles y no retiradas. Por lo tanto, la serie reconstruye las fechas de ingreso de las publicaciones que hoy integran ese universo.

No constituye una fotografía exacta del estado administrativo que tenía el repositorio en cada fecha histórica. Un recurso retirado posteriormente no forma parte del conteo actual.

La suma acumulada comienza en el primer período devuelto por la consulta. Para representar toda la historia debe utilizarse **No filter** como rango temporal. Si se aplica un filtro que comienza en 2020, el acumulado empezará desde cero en 2020 y no incorporará las publicaciones anteriores.
