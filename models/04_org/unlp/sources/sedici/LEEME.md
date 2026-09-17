# SEDICI desde el recorrido historizado

Esta rama representa SEDICI mediante los modelos compartidos de DSpace que
atraviesan la arquitectura historizada del Data Warehouse.

El nombre `sedici` identifica actualmente al repositorio, aunque la fuente
técnica de los modelos de publicaciones es `dspacedb5`. Esa procedencia debe
quedar declarada en `schema.yml` y conservarse en los contratos derivados.

La rama es independiente del snapshot directo restaurado en `ldg_sedicidb`.
Ambas representaciones pueden coexistir y deben seleccionarse explícitamente
desde analytics.
