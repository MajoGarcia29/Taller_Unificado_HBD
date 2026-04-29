## Parte III — Integración con MongoDB y Análisis

### a) Almacenamiento en MongoDB con Prefect

Se integró el dataset unificado al pipeline de datos utilizando **Prefect**, permitiendo automatizar el proceso de carga hacia una base de datos NoSQL.
Se estableció una conexión con MongoDB Atlas y se almacenaron los registros en una colección, garantizando la persistencia de los datos procesados.

---

### b) Visualización georreferenciada

Se emplearon herramientas de visualización con Plotly para representar información a nivel global.
Se generaron mapas que permiten identificar:

* El video más visto por país
* La categoría con mayor número de visualizaciones por país

Esto facilita el análisis comparativo entre regiones y tendencias de consumo.

---

### c) Análisis de correlación

Se calculó la correlación entre el tiempo que tarda un video en volverse tendencia y su número de visualizaciones.
Además, se representó gráficamente esta relación mediante un diagrama de dispersión con línea de regresión (OLS), permitiendo observar la tendencia entre ambas variables.

---

###  Conclusión

La integración con MongoDB permitió almacenar y gestionar eficientemente los datos, mientras que las visualizaciones y el análisis de correlación facilitaron la comprensión del comportamiento de los videos en tendencia a nivel global.
