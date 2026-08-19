# Modelos organizacionales de CIC

CIC dispone de varias representaciones de sus publicaciones. Cada fuente se
modela en una rama independiente para conservar su alcance y procedencia.

## Fuentes

```text
cic/
├── cicdigital/  # snapshot directo de la base del repositorio
├── dspacedb/    # representación compartida e historizada de DSpace
├── openaire/    # productos recuperados desde OpenAIRE
└── openalex/    # obras recuperadas desde OpenAlex
```

Las ramas pueden coexistir y producir contratos comparables. Un análisis debe
elegir explícitamente cuál utiliza; no se define una fuente institucional única
por configuración del target.

## Publicaciones y deduplicación

La publicación de un ítem en un repositorio es el hecho principal para las
ramas DSpace. Las adaptaciones para deduplicación deben mantener el nombre de la
fuente que las origina, por ejemplo:

```text
fct_cic_cicdigital_dedup_publication
fct_cic_dspacedb_dedup_publication
```

Ambos contratos identifican al repositorio mediante `source` y conservan el
recorrido técnico mediante `source_system`.

Un contrato institucional sin fuente sólo se justifica si representa una unión
explícita, conserva una columna de procedencia y define cómo trata registros
repetidos entre fuentes.
