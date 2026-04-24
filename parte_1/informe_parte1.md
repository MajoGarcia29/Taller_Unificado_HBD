# Informe de Hallazgos — Parte I
## Pipeline de Datos: YouTube Trending (datasnaek/youtube-new)

---

## 1. Ingesta de datos

El dataset contiene registros de tendencias de YouTube para 10 países: US, GB, CA, DE, FR, RU, MX, KR, JP e IN. Tras la descarga y carga de los archivos, se obtuvieron **239,662 filas en total**. Sin embargo, los archivos correspondientes a Japón, Corea del Sur, México y Rusia presentaron errores de codificación (caracteres no compatibles con UTF-8), por lo que fueron omitidos en esta iteración, quedando **6 países activos** con un total de **238,662 filas procesadas**.

---

## 2. Unificación y selección del Top 100 global

Tras unificar los datasets por país y eliminar duplicados por `video_id`, se seleccionaron los 100 vídeos más vistos a escala global. El umbral mínimo de visualizaciones para entrar al top 100 fue de **36,957,773 vistas**.

El vídeo más visto fue **Nicky Jam x J. Balvin - X (EQUIS)** con **424,538,912 reproducciones**, registrado desde el dataset de Gran Bretaña. El contenido dominante en el top 100 corresponde a videoclips musicales, con predominancia de artistas latinoamericanos y de pop anglosajón, seguidos por tráilers cinematográficos y contenido viral.

---

## 3. Estandarización de fechas

El dataset presentó dos formatos de fecha distintos que requirieron transformación:

- `trending_date` en formato `YY.DD.MM` (ej. `17.14.11`), no estándar y propenso a confusión con otros formatos de fecha.
- `publish_time` en formato ISO 8601 con zona horaria UTC (ej. `2018-03-02T05:00:19.000Z`).

Ambos campos fueron estandarizados exitosamente a objetos `datetime` sin zona horaria, sin valores nulos resultantes (`NaT: 0`) en ninguno de los dos métodos aplicados. Se verificó además que los resultados obtenidos mediante `apply` + lambda y mediante `iterrows` fueron idénticos, validando la consistencia de ambos enfoques.

---

## 4. Duración entre publicación y tendencia

Se calculó la diferencia en días entre la fecha de publicación del vídeo y la fecha en que apareció como tendencia. Los resultados muestran el siguiente comportamiento:

| Estadistica | Valor |
|-------------|-------|
| Media | 26.7 dias |
| Mediana | 30.0 dias |
| Minimo | 5 dias |
| Maximo | 37 dias |

La distribución por categoría de velocidad fue la siguiente:

| Categoria | Videos | Descripcion |
|-----------|--------|-------------|
| lento | 49 | Mas de 30 dias |
| moderado | 47 | Entre 8 y 30 dias |
| rapido | 4 | Entre 2 y 7 dias |
| viral_inmediato | 0 | Menos de 1 dia |

El hallazgo más relevante es que casi la totalidad de los vídeos del top 100 tardó entre 8 y 37 días en convertirse en tendencia, lo que sugiere que los vídeos más vistos no necesariamente son los que viralizan más rápido, sino los que sostienen una alta acumulación de vistas a lo largo de varias semanas. Ningún vídeo del top 100 alcanzó la categoría viral_inmediato, lo que refuerza esta observación.

---

## 5. Conclusiones

El pipeline procesó exitosamente el dataset de YouTube Trending, unificó registros de múltiples países y enriqueció el dataset con métricas de temporalidad. Los principales hallazgos indican que el contenido musical latino y de pop anglosajón domina las tendencias globales en el periodo analizado (2017-2018), y que los vídeos con mayor volumen de vistas tienden a consolidarse gradualmente en lugar de viralizarse de forma inmediata.

Como mejora pendiente, se debe corregir el encoding de los archivos de JP, KR, MX y RU para incluirlos en futuras ejecuciones del pipeline.
