# Contribuir

## Convención de commits

Formato sugerido:

`<tipo>(<scope>): <mensaje>`

Tipos recomendados:

- `feat`: nueva funcionalidad
- `fix`: corrección de bug o ajuste semántico
- `refactor`: cambio estructural sin cambio funcional intencional
- `docs`: documentación
- `test`: tests
- `chore`: mantenimiento
- `perf`: mejora de performance
- `build`: cambios de dependencias o build
- `ci`: cambios de integración o despliegue

Scopes sugeridos para este repositorio:

- Capas: `ldg`, `dv`, `dm-core`, `dm-audit`, `viz`
- Fuentes o dominios: `openalex`, `openaire`, `oai`, `dspacedb`, `dspacedb5`

Ejemplos:

- `feat(dm-core): agrega fct_entity_publication`
- `refactor(dm-core): desacopla latest satellites y extraction facts`
- `fix(dm-core): preserva institution_ror como contexto de extracción`
- `docs(dm-core): aclara semántica de institution_ror en entity facts`

## Notas de modelado

- Mantener las hash keys nativas (`*_hk`) en su tipo técnico original siempre que no haya una razón clara para exponer una representación textual derivada.
- En modelos transversales de `dm`, distinguir relación bibliográfica de contexto de extracción. En OpenAlex y OpenAIRE, `institution_ror` puede representar el ROR usado para recuperar la entidad y no una afiliación intrínseca de la publicación.
- Mantener los hechos base en `models/03_dm/core/` y las agregaciones operativas en `models/03_dm/audit/`.
