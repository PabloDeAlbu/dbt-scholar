# AGENTS.md

## dbt / Data Vault guardrails

- Nunca proponer ni ejecutar `dbt run --full-refresh` sobre modelos de `models/02_dv/**`.
- En este proyecto, `ldg` representa solo el ultimo batch disponible por entidad; no es historico completo.
- Asumir que un full refresh en DV puede truncar historia valida y romper consistencia aguas abajo.
- Ante cambios de modelado en DV, preferir migraciones compatibles hacia adelante, ajustes en `dm`, o instrucciones especificas del usuario.
- Si una solucion pareciera requerir full refresh en DV, detenerse y pedir confirmacion explicita con explicacion del impacto.
