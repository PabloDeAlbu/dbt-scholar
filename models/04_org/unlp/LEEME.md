# Modelos organizacionales de UNLP

UNLP puede representarse desde varias fuentes institucionales y externas. Las
ramas de `sources` conservan su sistema de origen y aplican únicamente las
reglas necesarias para delimitar el universo UNLP.

Entre las fuentes actuales se encuentran:

- `sedicidb`: snapshot directo de la base DSpace 5 de SEDICI;
- `sedici`: representación construida sobre el recorrido compartido e historizado;
- `openalex` y `openaire`: representaciones desde fuentes externas;
- `googlescholar`: perfiles recuperados desde Google Scholar;
- `vocsedici`: autoridades administradas por SEDICI.

`integration` reúne correspondencias explícitas entre entidades observadas en
fuentes diferentes. Cada relación debe conservar su regla, su evidencia y la
posibilidad de reconocer casos ambiguos.

Los hechos y dimensiones ubicados en la raíz de `unlp` representan resultados
institucionales reutilizables. No reemplazan a las fuentes que los componen.

Los modelos analytics pueden elegir una fuente concreta o un resultado
integrado según la pregunta que respondan. Si se comparan dos representaciones
de SEDICI, ambas deben conservar columnas que permitan identificar el sistema
fuente y el momento observado.
