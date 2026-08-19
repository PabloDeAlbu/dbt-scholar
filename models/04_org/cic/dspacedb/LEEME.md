# CIC desde dspacedb

Esta rama delimita CIC dentro de los modelos compartidos de `dspacedb`. La
institución se identifica por el ROR `https://ror.org/02s7sax82` y la fuente
conserva contexto de repositorio, extracción y carga.

```text
modelos compartidos dspacedb
    -> filtro y reglas institucionales CIC
    -> fct_cic_dspacedb_item_publication
```

A diferencia de `cicdigital`, esta rama se apoya en el recorrido historizado
del Data Warehouse. Debe utilizarse cuando el análisis necesita comparar
extracciones, conservar procedencia temporal o trabajar con varias instancias
DSpace bajo el mismo contrato técnico.

La rama no debe depender de que exista un dump directo en `ldg_cicdigital`.
