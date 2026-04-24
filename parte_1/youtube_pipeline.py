"""
Pipeline Completo de Datos - YouTube Trending (datasnaek/youtube_new)
======================================================================
Parte I — Orquestación con Prefect 3

"""

import os
import glob
import zipfile
import logging
from pathlib import Path
from datetime import datetime

import pandas as pd
from prefect import flow, task, get_run_logger

# ─────────────────────────────────────────────
#  CONFIGURACIÓN GLOBAL
# ─────────────────────────────────────────────

# Dataset Kaggle: datasnaek/youtube-new
KAGGLE_DATASET  = "datasnaek/youtube-new"
STAGING_DIR     = Path("staging/youtube_raw")        # zona de staging (raw)
PROCESSED_DIR   = Path("staging/youtube_processed")  # zona de staging (procesado)

# Países disponibles en el dataset
COUNTRY_CODES = ["US", "GB", "CA", "DE", "FR", "RU", "MX", "KR", "JP", "IN"]

TOP_N = 100  # top N vídeos más vistos globalmente

# ═══════════════════════════════════════════════════════════════
#  a) DESCARGA DE DATOS A STAGING LOCAL
# ═══════════════════════════════════════════════════════════════

@task(name="crear_directorios_staging", retries=0)
def crear_directorios_staging() -> None:
    """Crea la estructura de carpetas de la zona de staging."""
    logger = get_run_logger()
    STAGING_DIR.mkdir(parents=True, exist_ok=True)
    PROCESSED_DIR.mkdir(parents=True, exist_ok=True)
    logger.info(f"Directorios de staging listos: {STAGING_DIR}, {PROCESSED_DIR}")


@task(name="descargar_dataset_kaggle", retries=2, retry_delay_seconds=10)
def descargar_dataset_kaggle() -> list[Path]:
    logger = get_run_logger()

    try:
        import kaggle  # noqa: F401
        kaggle.api.authenticate()
        api = kaggle.api


        zip_path = STAGING_DIR / "youtube-new.zip"
        logger.info(f"Descargando {KAGGLE_DATASET} → {STAGING_DIR} …")
        api.dataset_download_files(KAGGLE_DATASET, path=str(STAGING_DIR), unzip=False)

        # Descomprimir
        with zipfile.ZipFile(zip_path, "r") as z:
            z.extractall(STAGING_DIR)
        zip_path.unlink(missing_ok=True)
        logger.info("Dataset descomprimido correctamente.")

    except (Exception, SystemExit) as e:
        logger.warning(
            f"No se pudo descargar desde Kaggle ({e}). "
            "Generando datos de muestra sintéticos para demostración…"
        )
        _generar_datos_sinteticos()

    # Listar archivos CSV descargados
    csvs = sorted(STAGING_DIR.glob("*videos.csv"))
    if not csvs:
        raise FileNotFoundError(
            f"No se encontraron archivos *videos.csv en {STAGING_DIR}"
        )
    logger.info(f"Archivos CSV en staging: {[f.name for f in csvs]}")
    return csvs


def _generar_datos_sinteticos() -> None:
    import random, string

    random.seed(42)
    n_por_pais = 200  # filas por país

    categorias = list(range(1, 30))

    for code in COUNTRY_CODES:
        rows = []
        for i in range(n_por_pais):
            # trending_date en formato original del dataset: YY.DD.MM
            base_dt = datetime(2017 + random.randint(0, 1),
                               random.randint(1, 12),
                               random.randint(1, 28))
            trending_date = base_dt.strftime("%y.%d.%m")

            # publish_time en ISO 8601
            pub_dt = datetime(base_dt.year,
                              random.randint(1, base_dt.month),
                              random.randint(1, 28),
                              random.randint(0, 23),
                              random.randint(0, 59), 0)
            publish_time = pub_dt.strftime("%Y-%m-%dT%H:%M:%S.000Z")

            rows.append({
                "video_id":        "".join(random.choices(string.ascii_letters, k=11)),
                "trending_date":   trending_date,
                "title":           f"Video {code} #{i:03d}",
                "channel_title":   f"Channel_{random.randint(1, 50)}",
                "category_id":     random.choice(categorias),
                "publish_time":    publish_time,
                "tags":            "tag1|tag2",
                "views":           random.randint(50_000, 50_000_000),
                "likes":           random.randint(1_000, 2_000_000),
                "dislikes":        random.randint(100, 100_000),
                "comment_count":   random.randint(500, 500_000),
                "thumbnail_link":  "https://example.com/thumb.jpg",
                "comments_disabled": False,
                "ratings_disabled":  False,
                "video_error_or_removed": False,
                "description":     "Descripción del vídeo de muestra.",
            })

        df = pd.DataFrame(rows)
        df.to_csv(STAGING_DIR / f"{code}videos.csv",
                  index=False, encoding="utf-8")


# ═══════════════════════════════════════════════════════════════
#  b) UNIFICAR PAÍSES → TOP 100 GLOBAL
# ═══════════════════════════════════════════════════════════════

@task(name="cargar_y_unificar_paises", retries=1)
def cargar_y_unificar_paises(csv_paths: list[Path]) -> pd.DataFrame:

    logger = get_run_logger()
    frames = []

    for path in csv_paths:
        country_code = path.stem.replace("videos", "")  
        try:
            df = pd.read_csv(
                path,
                encoding="utf-8",
                on_bad_lines="skip",   
            )
            df["country"] = country_code
            frames.append(df)
            logger.info(f"  {country_code}: {len(df):,} filas cargadas")
        except Exception as e:
            logger.warning(f"  Error leyendo {path.name}: {e} — se omite.")

    if not frames:
        raise ValueError("No se cargó ningún DataFrame de país.")

    unified = pd.concat(frames, ignore_index=True)
    logger.info(f"Dataset unificado: {len(unified):,} filas totales")

    unified["views"] = pd.to_numeric(unified["views"], errors="coerce")

    # Top 100 vídeos más vistos a escala global
    top100 = (
        unified
        .sort_values("views", ascending=False)
        .drop_duplicates(subset="video_id")   # un vídeo puede aparecer en varios países
        .head(TOP_N)
        .reset_index(drop=True)
    )

    logger.info(f"Top {TOP_N} global seleccionados. Mínimo de views: {top100['views'].min():,}")
    return top100


# ═══════════════════════════════════════════════════════════════
#  c) ESTANDARIZACIÓN DE FECHAS
# ═══════════════════════════════════════════════════════════════

# ── Funciones auxiliares de parseo ─────────────────────────────

def _parsear_trending_date(valor: str):
    try:
        return pd.Timestamp(datetime.strptime(str(valor), "%y.%d.%m"))
    except (ValueError, TypeError):
        return pd.NaT


def _parsear_publish_time(valor: str):
    try:
        return pd.to_datetime(str(valor), utc=True).tz_localize(None)
    except (ValueError, TypeError):
        return pd.NaT


@task(name="estandarizar_fechas_apply_lambda")
def estandarizar_fechas_apply_lambda(df: pd.DataFrame) -> pd.DataFrame:

    logger = get_run_logger()
    logger.info("Estandarizando fechas con apply + lambda …")

    df = df.copy()

    # trending_date: YY.DD.MM → datetime
    df["trending_date_std"] = df["trending_date"].apply(
        lambda v: _parsear_trending_date(v)
    )

    # publish_time: ISO 8601 → datetime (sin timezone)
    df["publish_time_std"] = df["publish_time"].apply(
        lambda v: _parsear_publish_time(v)
    )

    nulos_td = df["trending_date_std"].isna().sum()
    nulos_pt = df["publish_time_std"].isna().sum()
    logger.info(
        f"  trending_date NaT: {nulos_td}  |  publish_time NaT: {nulos_pt}"
    )
    return df


@task(name="estandarizar_fechas_iterrows")
def estandarizar_fechas_iterrows(df: pd.DataFrame) -> pd.DataFrame:

    logger = get_run_logger()
    logger.info("Estandarizando fechas con iterrows …")

    trending_dates_std = []
    publish_times_std  = []

    for idx, row in df.iterrows():
        # trending_date
        td_raw = row.get("trending_date", "")
        td_std = _parsear_trending_date(td_raw)
        trending_dates_std.append(td_std)

        # publish_time
        pt_raw = row.get("publish_time", "")
        pt_std = _parsear_publish_time(pt_raw)
        publish_times_std.append(pt_std)

    df = df.copy()
    df["trending_date_std_iter"] = trending_dates_std
    df["publish_time_std_iter"]  = publish_times_std

    logger.info(
        f"  Procesadas {len(df)} filas con iterrows. "
        f"NaT trending: {pd.isna(df['trending_date_std_iter']).sum()}  "
        f"NaT publish: {pd.isna(df['publish_time_std_iter']).sum()}"
    )
    return df


# ═══════════════════════════════════════════════════════════════
#  d) DURACIÓN ENTRE CARGA Y TENDENCIA
# ═══════════════════════════════════════════════════════════════

@task(name="calcular_duracion_tendencia")
def calcular_duracion_tendencia(df: pd.DataFrame) -> pd.DataFrame:

    logger = get_run_logger()
    logger.info("Calculando duración entre carga y tendencia …")

    df = df.copy()

    # Diferencia como timedelta
    df["delta_tendencia"] = df["trending_date_std"] - df["publish_time_std"]

    # Convertir a unidades concretas
    df["dias_hasta_tendencia"]  = df["delta_tendencia"].dt.days
    df["horas_hasta_tendencia"] = df["delta_tendencia"].dt.total_seconds() / 3600

    # Etiqueta de velocidad de viralización
    def categorizar_velocidad(dias):
        if pd.isna(dias):
            return "desconocido"
        elif dias <= 1:
            return "viral_inmediato"   # ≤ 1 día
        elif dias <= 7:
            return "rapido"            # 2–7 días
        elif dias <= 30:
               return "moderado"       # 8–30 días
        else:
            return "lento"             # > 30 días

    df["categoria_velocidad"] = df["dias_hasta_tendencia"].apply(
        lambda d: categorizar_velocidad(d)
    )

    # Resumen estadístico
    stats = df["dias_hasta_tendencia"].describe()
    logger.info(
        f"\n{'─'*45}\n"
        f"  Días hasta tendencia — estadísticas:\n"
        f"  Media  : {stats['mean']:.1f} días\n"
        f"  Mediana: {stats['50%']:.1f} días\n"
        f"  Mín    : {stats['min']:.0f} días\n"
        f"  Máx    : {stats['max']:.0f} días\n"
        f"{'─'*45}"
    )

    dist = df["categoria_velocidad"].value_counts().to_dict()
    logger.info(f"  Distribución de velocidad: {dist}")

    return df


# ═══════════════════════════════════════════════════════════════
#  GUARDAR RESULTADO
# ═══════════════════════════════════════════════════════════════

@task(name="guardar_dataset_procesado")
def guardar_dataset_procesado(df: pd.DataFrame) -> Path:
    logger = get_run_logger()

    output_path = PROCESSED_DIR / "youtube_top100_global.csv"
    df.to_csv(output_path, index=False, encoding="utf-8")
    logger.info(f"Dataset guardado en: {output_path}  ({len(df)} filas, {len(df.columns)} columnas)")

    cols_reporte = [
        "video_id", "title", "country", "views",
        "publish_time_std", "trending_date_std",
        "dias_hasta_tendencia", "categoria_velocidad",
    ]
    cols_disponibles = [c for c in cols_reporte if c in df.columns]
    logger.info(f"\n{df[cols_disponibles].to_string(index=True)}")

    return output_path


# ═══════════════════════════════════════════════════════════════
#  FLOW PRINCIPAL
# ═══════════════════════════════════════════════════════════════

@flow(
    name="youtube_trending_pipeline",
    description=(
        "Pipeline completo para datos de YouTube Trending (Kaggle). "
        "Descarga, unifica, estandariza fechas y calcula duración hasta tendencia."
    ),
    log_prints=True,
)
def youtube_trending_pipeline():

    # ── a) Staging ───────────────────────────────────────────
    crear_directorios_staging()
    csv_paths = descargar_dataset_kaggle()

    # ── b) Unificación ───────────────────────────────────────
    df_top100 = cargar_y_unificar_paises(csv_paths)

    # ── c) Estandarización (dos métodos)  ────────────────────
    # Método 1: apply + lambda (columnas: *_std)
    df_fechas_apply = estandarizar_fechas_apply_lambda(df_top100)

    # Método 2: iterrows (columnas: *_iter — verificación comparativa)
    df_fechas_iter  = estandarizar_fechas_iterrows(df_fechas_apply)

    # Verificar coherencia entre ambos métodos
    assert (
        df_fechas_iter["trending_date_std"].equals(df_fechas_iter["trending_date_std_iter"])
    ), "¡Discrepancia entre apply+lambda e iterrows en trending_date!"
    assert (
        df_fechas_iter["publish_time_std"].equals(df_fechas_iter["publish_time_std_iter"])
    ), "¡Discrepancia entre apply+lambda e iterrows en publish_time!"

    # ── d) Duración hasta tendencia  ─────────────────────────
    df_final = calcular_duracion_tendencia(df_fechas_iter)

    # ── Persistencia  ────────────────────────────────────────
    output_path = guardar_dataset_procesado(df_final)

    return str(output_path)


# ─────────────────────────────────────────────
#  PUNTO DE ENTRADA
# ─────────────────────────────────────────────

if __name__ == "__main__":
    resultado = youtube_trending_pipeline()
    print(f"\n Pipeline completado. Resultado en: {resultado}")
