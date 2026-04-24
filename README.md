# YouTube Trending Data Pipeline

Pipeline completo de datos para el dataset de tendencias de YouTube en Kaggle ([datasnaek/youtube-new](https://www.kaggle.com/datasets/datasnaek/youtube-new)), orquestado con **Prefect 3**.

---

## Parte I — Pipeline de ingesta y transformación

### ¿Qué hace el pipeline?

**a) Descarga a staging local**
Descarga automáticamente los archivos CSV del dataset desde Kaggle usando la API oficial y los almacena en una zona local de staging (`staging/youtube_raw/`). Si no hay credenciales disponibles, genera datos sintéticos con la misma estructura para permitir pruebas sin conexión.

**b) Unificación de países → Top 100 global**
Lee los CSVs de cada país (US, GB, CA, DE, FR, RU, MX, KR, JP, IN), agrega una columna `country` a cada uno y los concatena en un único DataFrame. Luego selecciona los **100 vídeos más vistos globalmente**, eliminando duplicados por `video_id`.

**c) Estandarización de fechas**
Estandariza las fechas del dataset usando dos métodos distintos:
- **`apply` + funciones lambda** — procesamiento vectorizado sobre toda la columna
- **`iterrows`** — iteración explícita fila a fila

El formato original de `trending_date` es `YY.DD.MM` (ej. `17.14.11`) y se convierte a `datetime` estándar. El campo `publish_time` viene en ISO 8601 y se normaliza eliminando la zona horaria.

**d) Duración entre carga y tendencia**
Calcula cuántos días tardó cada vídeo en convertirse en tendencia desde su publicación. Agrega las columnas:
- `dias_hasta_tendencia` — diferencia en días
- `horas_hasta_tendencia` — diferencia en horas
- `categoria_velocidad` — etiqueta cualitativa: `viral_inmediato` (≤1d), `rapido` (2–7d), `moderado` (8–30d), `lento` (>30d)

---

## Estructura del proyecto

```
├── youtube_pipeline.py            # Pipeline principal (Prefect)
├── staging/
│   ├── youtube_raw/               # CSVs descargados desde Kaggle
│   └── youtube_processed/
│       └── youtube_top100_global.csv   # Dataset final procesado
└── README.md
```

---

## Requisitos

- Python 3.11
- Miniconda / Anaconda

---

## Instalación

```bash
# Crear y activar entorno
conda create -n youtube_pipeline python=3.11
conda activate youtube_pipeline

# Instalar dependencias
pip install prefect kaggle pandas

# Configurar variable de entorno para Prefect (una sola vez)
conda env config vars set PREFECT_SERVER_ALLOW_EPHEMERAL_MODE=true -n youtube_pipeline
conda deactivate && conda activate youtube_pipeline
```

---

## Configuración de Kaggle

1. Ir a [https://www.kaggle.com/settings](https://www.kaggle.com/settings)
2. Sección **API** → **Create New Token**
3. Mover el archivo `kaggle.json` descargado a:

```
# Windows
C:\Users\TuNombre\.kaggle\kaggle.json
```

---

## Ejecución

```bash
python youtube_pipeline.py
```

El pipeline descarga los datos, los procesa y guarda el resultado en `staging/youtube_processed/youtube_top100_global.csv`.

---

## Resultado esperado

```
Pipeline completado. Resultado en: staging\youtube_processed\youtube_top100_global.csv
```

El dataset final contiene **100 filas** y **25 columnas**, incluyendo:

| Columna | Descripción |
|---------|-------------|
| `video_id` | ID único del vídeo |
| `title` | Título del vídeo |
| `country` | País de origen del registro |
| `views` | Número de visualizaciones |
| `publish_time_std` | Fecha de publicación estandarizada |
| `trending_date_std` | Fecha de tendencia estandarizada |
| `dias_hasta_tendencia` | Días entre publicación y tendencia |
| `categoria_velocidad` | Velocidad de viralización |

---

## Tecnologías utilizadas

- [Prefect 3](https://docs.prefect.io/) — orquestación del pipeline
- [Kaggle API](https://github.com/Kaggle/kaggle-api) — descarga del dataset
- [pandas](https://pandas.pydata.org/) — procesamiento de datos
