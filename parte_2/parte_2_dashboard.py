"""
YouTube Trending — Parte II
Dashboard interactivo con Plotly
Orquestado como flow Prefect 3 (compatible con la Parte I)
"""

import json
import os
from pathlib import Path

import pandas as pd
import plotly.graph_objects as go
import plotly.express as px
from plotly.subplots import make_subplots

# ─── Rutas (misma convención que la Parte I) ─────────────────────

BASE_DIR      = Path(__file__).parent.parent
PROCESSED_CSV = BASE_DIR / "staging" / "youtube_processed" / "youtube_top100_global.csv"
RAW_DIR       = BASE_DIR / "staging" / "youtube_raw"
OUTPUT_HTML   = Path(__file__).parent / "dashboard_youtube.html"
# ─── Mapa de categorías ──────────────────────────────────────────
CAT_MAP = {
    1:  "Film & Animation",  10: "Music",
    22: "People & Blogs",    23: "Comedy",
    24: "Entertainment",     26: "Howto & Style",
    28: "Science & Tech",
}

COUNTRY_NAMES = {
    "US": "United States", "GB": "United Kingdom",
    "CA": "Canada",        "DE": "Germany",
    "FR": "France",        "RU": "Russia",
    "MX": "Mexico",        "KR": "South Korea",
    "JP": "Japan",         "IN": "India",
}

# Colores
CAT_COLORS = {
    "Music":            "#58a6ff",
    "Entertainment":    "#f78166",
    "People & Blogs":   "#3fb950",
    "Comedy":           "#ffa657",
    "Howto & Style":    "#d2a8ff",
    "Science & Tech":   "#79c0ff",
    "Film & Animation": "#ff7b72",
}

#Cargar y enriquecer datos
# 
def cargar_datos(csv_path: Path) -> pd.DataFrame:
    df = pd.read_csv(csv_path)

    # Mapear categoría y nombre de país
    df["category_name"] = df["category_id"].map(CAT_MAP).fillna("Other")
    df["country_name"]  = df["country"].map(COUNTRY_NAMES).fillna(df["country"])

    # Polaridad de interacción
    df["polaridad"]       = df["likes"] - df["dislikes"]
    df["polaridad_pct"]   = (df["polaridad"] / (df["likes"] + df["dislikes"] + 1) * 100).round(1)
    df["pol_label"]       = df["polaridad"].apply(lambda x: "Positiva ✔" if x >= 0 else "Negativa ✘")

    # Abreviar títulos largos para etiquetas
    df["title_short"] = df["title"].apply(lambda t: t[:48] + "…" if len(t) > 48 else t)

    return df



#  TASK 2 — Cargar JSONs de categorías (datos en formato JSON)

def cargar_categorias_json(raw_dir: Path) -> dict:
    """
    Lee los archivos *_category_id.json de staging/youtube_raw/
    y devuelve un dict unificado {cat_id: cat_name}.
    """
    cat_map = {}
    for jf in raw_dir.glob("*_category_id.json"):
        try:
            with open(jf, encoding="utf-8") as f:
                data = json.load(f)
            for item in data.get("items", []):
                cid  = int(item["id"])
                name = item["snippet"]["title"]
                cat_map[cid] = name
        except Exception:
            pass
    return cat_map


# Figuras


DARK = "#0d1117"
SURF = "#161b22"
BORD = "#30363d"
TEXT = "#e6edf3"
MUTED= "#8b949e"

def _base_layout(**kwargs):
    return dict(
        plot_bgcolor=DARK, paper_bgcolor=DARK,
        font=dict(color=TEXT, family="'Space Mono', monospace", size=12),
        margin=dict(t=65, b=55, l=50, r=30),
        **kwargs
    )


#  Top videos por país (barras horizontales) 
def fig_top_videos_por_pais(df: pd.DataFrame) -> go.Figure:
    top = (df.sort_values("views", ascending=False)
             .drop_duplicates("video_id")
             .head(20)
             .sort_values("views"))

    colors = [CAT_COLORS.get(c, "#58a6ff") for c in top["category_name"]]

    fig = go.Figure(go.Bar(
        x=top["views"] / 1e6,
        y=top["title_short"],
        orientation="h",
        marker=dict(color=colors, line=dict(width=0)),
        customdata=top[["country_name", "category_name", "views", "channel_title"]].values,
        hovertemplate=(
            "<b>%{y}</b><br>"
            "País: %{customdata[0]}<br>"
            "Categoría: %{customdata[1]}<br>"
            "Vistas: %{customdata[2]:,}<br>"
            "Canal: %{customdata[3]}<extra></extra>"
        ),
    ))

    # Anotaciones de país a la derecha
    for _, row in top.iterrows():
        fig.add_annotation(
            x=row["views"] / 1e6, y=row["title_short"],
            text=f"  {row['country']}",
            showarrow=False, xanchor="left",
            font=dict(size=9, color=MUTED),
        )

    fig.update_layout(
        title=dict(text="🎬 Top 20 Vídeos Más Vistos (Global)", font=dict(size=15)),
        xaxis=dict(title="Millones de vistas", gridcolor=BORD, tickformat=".0f"),
        yaxis=dict(tickfont=dict(size=9), gridcolor=BORD),
        height=560,
        **_base_layout(),
    )
    return fig


#Vistas por país 
def fig_vistas_por_pais(df: pd.DataFrame) -> go.Figure:
    agg = (df.groupby("country_name")
             .agg(views=("views","sum"), videos=("video_id","count"))
             .reset_index()
             .sort_values("views", ascending=False))

    fig = go.Figure()
    fig.add_trace(go.Bar(
        x=agg["country_name"], y=agg["views"] / 1e9,
        name="Vistas (B)",
        marker=dict(
            color=agg["views"],
            colorscale="Blues",
            showscale=False,
            line=dict(width=0),
        ),
        customdata=agg[["videos", "views"]].values,
        hovertemplate="<b>%{x}</b><br>Vistas: %{customdata[1]:,}<br>Videos: %{customdata[0]}<extra></extra>",
    ))
    fig.add_trace(go.Scatter(
        x=agg["country_name"], y=agg["videos"],
        name="N° Videos", yaxis="y2",
        mode="markers+lines",
        marker=dict(size=10, color="#ffa657", symbol="diamond"),
        line=dict(color="#ffa657", width=2, dash="dot"),
        hovertemplate="<b>%{x}</b><br>Videos en top 100: %{y}<extra></extra>",
    ))
    fig.update_layout(
        title=dict(text="🌍 Vistas Totales y Videos por País", font=dict(size=15)),
        xaxis=dict(tickangle=-25, gridcolor=BORD),
        yaxis=dict(title="Miles de millones", gridcolor=BORD),
        yaxis2=dict(title="N° Videos", overlaying="y", side="right", showgrid=False),
        legend=dict(orientation="h", y=1.08, x=0.5, xanchor="center",
                    bgcolor="rgba(0,0,0,0)", bordercolor=BORD),
        height=420,
        **_base_layout(),
    )
    return fig


# Categorías por país — heatmap con datos JSON 
def fig_categorias_por_pais(df: pd.DataFrame) -> go.Figure:
    pivot = (df.groupby(["country_name","category_name"])["views"]
               .sum().reset_index()
               .pivot(index="category_name", columns="country_name", values="views")
               .fillna(0))

    # texto en millones
    text_vals = (pivot.values / 1e6).round(0).astype(int)
    text_fmt  = [[f"{v}M" if v > 0 else "" for v in row] for row in text_vals]

    fig = go.Figure(go.Heatmap(
        z=pivot.values / 1e6,
        x=pivot.columns.tolist(),
        y=pivot.index.tolist(),
        text=text_fmt,
        texttemplate="%{text}",
        textfont=dict(size=10),
        colorscale="YlOrRd",
        colorbar=dict(title="Vistas (M)", tickformat=".0f"),
        hovertemplate="País: %{x}<br>Categoría: %{y}<br>Vistas: %{z:.0f}M<extra></extra>",
    ))
    fig.update_layout(
        title=dict(text="📊 Vistas por Categoría y País (Millones) — datos JSON", font=dict(size=15)),
        xaxis=dict(tickangle=-25, gridcolor=BORD),
        yaxis=dict(autorange="reversed", gridcolor=BORD),
        height=380,
        **_base_layout(),
    )
    return fig


# Interacción — vistas / likes / dislikes
def fig_interaccion_zona(df: pd.DataFrame) -> go.Figure:
    agg = (df.groupby("country_name")
             .agg(views=("views","sum"), likes=("likes","sum"), dislikes=("dislikes","sum"))
             .reset_index().sort_values("views", ascending=False))
    agg["pol_pct"] = ((agg["likes"] - agg["dislikes"]) / (agg["likes"] + agg["dislikes"] + 1) * 100).round(1)

    fig = make_subplots(specs=[[{"secondary_y": True}]])
    s = 1e6
    for col, color, name in [
        ("views",    "#58a6ff", "Vistas (M)"),
        ("likes",    "#3fb950", "Likes (M)"),
        ("dislikes", "#f85149", "Dislikes (M)"),
    ]:
        fig.add_trace(go.Bar(
            name=name, x=agg["country_name"], y=agg[col] / s,
            marker=dict(color=color, opacity=0.85, line=dict(width=0)),
            hovertemplate=f"<b>%{{x}}</b><br>{name}: %{{y:.1f}}M<extra></extra>",
        ), secondary_y=False)

    fig.add_trace(go.Scatter(
        name="Polaridad %", x=agg["country_name"], y=agg["pol_pct"],
        mode="lines+markers+text",
        text=[f"{v:.0f}%" for v in agg["pol_pct"]],
        textposition="top center",
        textfont=dict(size=9, color="#ffa657"),
        marker=dict(size=9, color="#ffa657", symbol="circle"),
        line=dict(color="#ffa657", width=2),
        hovertemplate="<b>%{x}</b><br>Polaridad: %{y:.1f}%<extra></extra>",
    ), secondary_y=True)

    fig.update_layout(
        title=dict(text="❤️ Interacción por País — Vistas · Likes · Dislikes y Polaridad", font=dict(size=15)),
        barmode="group",
        xaxis=dict(tickangle=-25, gridcolor=BORD),
        legend=dict(orientation="h", y=1.08, x=0.5, xanchor="center",
                    bgcolor="rgba(0,0,0,0)", bordercolor=BORD),
        height=420,
        **_base_layout(),
    )
    fig.update_yaxes(title_text="Millones", secondary_y=False, gridcolor=BORD)
    fig.update_yaxes(title_text="Polaridad %", secondary_y=True, showgrid=False)
    return fig


# Polaridad por categoría — barras con colores neg/pos ─
def fig_polaridad_categoria(df: pd.DataFrame) -> go.Figure:
    agg = (df.groupby("category_name")
             .agg(likes=("likes","sum"), dislikes=("dislikes","sum"),
                  views=("views","sum"), n=("video_id","count"))
             .reset_index())
    agg["pol_pct"] = ((agg["likes"] - agg["dislikes"]) / (agg["likes"] + agg["dislikes"] + 1) * 100).round(1)
    agg = agg.sort_values("pol_pct", ascending=True)

    colors = ["#f85149" if v < 0 else "#3fb950" for v in agg["pol_pct"]]

    fig = go.Figure(go.Bar(
        x=agg["pol_pct"], y=agg["category_name"],
        orientation="h",
        marker=dict(color=colors, line=dict(width=0)),
        customdata=agg[["likes","dislikes","views","n"]].values,
        hovertemplate=(
            "<b>%{y}</b><br>"
            "Polaridad: %{x:.1f}%<br>"
            "Likes: %{customdata[0]:,}<br>"
            "Dislikes: %{customdata[1]:,}<br>"
            "Vistas: %{customdata[2]:,}<br>"
            "Videos: %{customdata[3]}<extra></extra>"
        ),
        text=[f"{v:.1f}%" for v in agg["pol_pct"]],
        textposition="outside",
        textfont=dict(size=10),
    ))
    fig.add_vline(x=0, line_color=MUTED, line_dash="dash", line_width=1)
    fig.update_layout(
        title=dict(text="⚡ Polaridad de Interacción por Categoría (% Likes − Dislikes)", font=dict(size=15)),
        xaxis=dict(title="Polaridad %", gridcolor=BORD, zeroline=False),
        yaxis=dict(gridcolor=BORD),
        height=360,
        **_base_layout(),
    )
    return fig


# Scatter polaridad por cat × país 
def fig_scatter_interaccion(df: pd.DataFrame) -> go.Figure:
    agg = (df.groupby(["country_name","category_name"])
             .agg(views=("views","sum"), likes=("likes","sum"), dislikes=("dislikes","sum"))
             .reset_index())
    agg["pol_pct"] = ((agg["likes"] - agg["dislikes"]) / (agg["likes"] + agg["dislikes"] + 1) * 100).round(1)

    fig = px.scatter(
        agg, x="country_name", y="category_name",
        size="views", color="pol_pct",
        color_continuous_scale="RdYlGn",
        color_continuous_midpoint=0,
        size_max=55,
        labels={"pol_pct":"Polaridad %","country_name":"País","category_name":"Categoría","views":"Vistas"},
        hover_data={"likes":True,"dislikes":True,"views":True,"pol_pct":True},
        template="plotly_dark",
    )
    fig.update_layout(
        title=dict(text="🔵 Grado de Interacción (tamaño=vistas, color=polaridad) por Zona y Categoría", font=dict(size=15)),
        xaxis=dict(tickangle=-25, gridcolor=BORD),
        yaxis=dict(gridcolor=BORD),
        coloraxis_colorbar=dict(title="Polaridad %"),
        height=420,
        **_base_layout(),
    )
    return fig


# Días hasta tendencia — violin
def fig_dias_tendencia(df: pd.DataFrame) -> go.Figure:
    fig = go.Figure()
    cats = df["categoria_velocidad"].unique().tolist()
    color_map = {"viral_inmediato":"#ffa657","rapido":"#3fb950","moderado":"#58a6ff","lento":"#f78166"}

    for cat in ["viral_inmediato","rapido","moderado","lento"]:
        sub = df[df["categoria_velocidad"] == cat]["dias_hasta_tendencia"]
        if sub.empty:
            continue
        fig.add_trace(go.Violin(
            y=sub, name=cat,
            box_visible=True, meanline_visible=True,
            line_color=color_map.get(cat,"#888"),
            fillcolor=color_map.get(cat,"#888"),
            opacity=0.6,
            points="all",
            pointpos=-1.2,
            marker=dict(size=5, opacity=0.7),
        ))

    fig.update_layout(
        title=dict(text="⏱️ Distribución de Días hasta Tendencia por Velocidad de Viralización", font=dict(size=15)),
        yaxis=dict(title="Días hasta tendencia", gridcolor=BORD),
        xaxis=dict(gridcolor=BORD),
        violingap=0.3, violinmode="overlay",
        height=380,
        **_base_layout(),
    )
    return fig


#  Treemap país › categoría
def fig_treemap(df: pd.DataFrame) -> go.Figure:
    agg = df.groupby(["country_name","category_name"])["views"].sum().reset_index()
    fig = px.treemap(
        agg, path=["country_name","category_name"],
        values="views",
        color="views", color_continuous_scale="Blues",
        hover_data={"views": True},
        template="plotly_dark",
    )
    fig.update_traces(
        hovertemplate="<b>%{label}</b><br>Vistas: %{value:,}<extra></extra>",
        textfont=dict(size=13),
    )
    fig.update_layout(
        title=dict(text="🗺️ Treemap de Vistas: País › Categoría", font=dict(size=15)),
        coloraxis_colorbar=dict(title="Vistas"),
        height=440,
        **_base_layout(),
    )
    return fig



#  Ensamblar HTML

def construir_dashboard(df: pd.DataFrame, cat_json: dict, out_path: Path) -> Path:
    out_path.parent.mkdir(parents=True, exist_ok=True)

    # KPIs
    total_views   = df["views"].sum()
    total_likes   = df["likes"].sum()
    total_dislikes= df["dislikes"].sum()
    pol_global    = (total_likes - total_dislikes) / (total_likes + total_dislikes + 1) * 100
    top_pais      = df.groupby("country_name")["views"].sum().idxmax()
    top_cat       = df.groupby("category_name")["views"].sum().idxmax()
    avg_dias      = df["dias_hasta_tendencia"].mean()

    kpi_html = f"""
    <div class="kpi-row">
      <div class="kpi"><span class="kpi-val">{total_views/1e9:.2f}B</span><span class="kpi-lbl">Vistas Totales</span></div>
      <div class="kpi"><span class="kpi-val">{total_likes/1e6:.1f}M</span><span class="kpi-lbl">Likes Totales</span></div>
      <div class="kpi"><span class="kpi-val">{total_dislikes/1e6:.1f}M</span><span class="kpi-lbl">Dislikes Totales</span></div>
      <div class="kpi {'kpi-pos' if pol_global>=0 else 'kpi-neg'}"><span class="kpi-val">{pol_global:.1f}%</span><span class="kpi-lbl">Polaridad Global</span></div>
      <div class="kpi"><span class="kpi-val">{top_pais}</span><span class="kpi-lbl">País Dominante</span></div>
      <div class="kpi"><span class="kpi-val">{top_cat}</span><span class="kpi-lbl">Categoría Líder</span></div>
      <div class="kpi"><span class="kpi-val">{avg_dias:.0f}d</span><span class="kpi-lbl">Días Prom. a Tendencia</span></div>
    </div>"""

    # Figuras con layout (span full / half)
    specs = [
        (fig_top_videos_por_pais(df),    "full"),
        (fig_vistas_por_pais(df),         "half"),
        (fig_interaccion_zona(df),         "half"),
        (fig_categorias_por_pais(df),     "full"),
        (fig_scatter_interaccion(df),     "full"),
        (fig_polaridad_categoria(df),     "half"),
        (fig_dias_tendencia(df),           "half"),
        (fig_treemap(df),                  "full"),
    ]

    charts_html = ""
    for i, (fig, span) in enumerate(specs):
        div = fig.to_html(full_html=False, include_plotlyjs=False, div_id=f"ch{i}")
        charts_html += f'<div class="card span-{span}">{div}</div>\n'

    # Categorías JSON como tabla
    cat_rows = "".join(
        f'<tr><td>{cid}</td><td>{name}</td></tr>'
        for cid, name in sorted(cat_json.items())
    )
    cat_table = f"""
    <div class="card span-full json-section">
      <h3 class="section-title">📂 Categorías desde JSON de Kaggle ({len(cat_json)} categorías)</h3>
      <table class="cat-table">
        <thead><tr><th>ID</th><th>Nombre</th></tr></thead>
        <tbody>{cat_rows}</tbody>
      </table>
    </div>"""

    html = f"""<!DOCTYPE html>
<html lang="es">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>YouTube Trending — Dashboard Parte II</title>
<script src="https://cdn.plot.ly/plotly-2.35.2.min.js"></script>
<link href="https://fonts.googleapis.com/css2?family=Space+Mono:wght@400;700&family=DM+Sans:wght@300;400;600;800&display=swap" rel="stylesheet">
<style>
:root{{
  --bg:#060a12; --surf:#0d1117; --surf2:#161b22;
  --bord:#21262d; --bord2:#30363d;
  --accent:#58a6ff; --accent2:#f78166;
  --green:#3fb950; --red:#f85149; --gold:#ffa657;
  --text:#e6edf3; --muted:#8b949e;
}}
*,*::before,*::after{{box-sizing:border-box;margin:0;padding:0}}
body{{background:var(--bg);color:var(--text);font-family:'DM Sans',sans-serif;min-height:100vh;overflow-x:hidden}}

/* ── HEADER ── */
header{{
  background:linear-gradient(135deg,#0d1117 0%,#0a0e17 100%);
  border-bottom:1px solid var(--bord2);
  padding:18px 40px;
  display:flex;align-items:center;gap:16px;
  position:sticky;top:0;z-index:100;
  backdrop-filter:blur(16px);
}}
.logo{{
  background:linear-gradient(135deg,#f85149,#f78166);
  color:#fff;padding:5px 14px;border-radius:6px;
  font-family:'Space Mono',monospace;font-size:10px;
  font-weight:700;letter-spacing:2px;text-transform:uppercase;
}}
header h1{{font-size:20px;font-weight:800;letter-spacing:-.3px}}
header h1 span{{color:var(--accent)}}
.header-right{{margin-left:auto;display:flex;gap:12px;align-items:center}}
.tag{{background:var(--surf2);border:1px solid var(--bord2);
      color:var(--muted);padding:3px 10px;border-radius:20px;
      font-family:'Space Mono',monospace;font-size:10px;}}
.tag-green{{border-color:var(--green);color:var(--green)}}

/* ── KPIs ── */
.kpi-row{{
  display:flex;gap:12px;flex-wrap:wrap;
  padding:20px 40px;
  background:var(--surf2);
  border-bottom:1px solid var(--bord);
}}
.kpi{{
  flex:1;min-width:110px;
  background:var(--surf);border:1px solid var(--bord);
  border-radius:10px;padding:14px 16px;
  display:flex;flex-direction:column;gap:4px;
  transition:border-color .2s,transform .15s;
}}
.kpi:hover{{border-color:var(--accent);transform:translateY(-2px)}}
.kpi-pos{{border-color:var(--green)!important}}
.kpi-neg{{border-color:var(--red)!important}}
.kpi-val{{font-size:22px;font-weight:800;color:var(--accent);
          font-family:'Space Mono',monospace;line-height:1.1}}
.kpi-pos .kpi-val{{color:var(--green)}}
.kpi-neg .kpi-val{{color:var(--red)}}
.kpi-lbl{{font-size:10px;color:var(--muted);text-transform:uppercase;letter-spacing:1px}}

/* ── GRID ── */
.grid{{
  display:grid;
  grid-template-columns:repeat(2,1fr);
  gap:16px;padding:28px 40px;
}}
.card{{
  background:var(--surf);border:1px solid var(--bord);
  border-radius:14px;overflow:hidden;padding:6px;
  transition:box-shadow .2s,border-color .2s;
}}
.card:hover{{
  box-shadow:0 0 0 1px var(--accent),0 8px 32px rgba(88,166,255,.08);
  border-color:var(--accent);
}}
.span-full{{grid-column:1/-1}}
.span-half{{grid-column:span 1}}

/* ── JSON TABLE ── */
.json-section{{padding:20px 24px}}
.section-title{{font-size:14px;font-weight:700;margin-bottom:14px;
                color:var(--gold);letter-spacing:.3px}}
.cat-table{{width:100%;border-collapse:collapse;font-size:12px}}
.cat-table th{{
  background:var(--surf2);color:var(--muted);
  padding:8px 12px;text-align:left;
  border-bottom:1px solid var(--bord2);
  font-family:'Space Mono',monospace;font-size:10px;
  text-transform:uppercase;letter-spacing:1px;
}}
.cat-table td{{
  padding:6px 12px;border-bottom:1px solid var(--bord);
  color:var(--text);
}}
.cat-table tr:hover td{{background:var(--surf2)}}
.cat-table tbody tr:nth-child(even) td{{background:rgba(255,255,255,.02)}}

/* ── FOOTER ── */
footer{{
  text-align:center;padding:20px;
  font-size:11px;color:var(--muted);
  font-family:'Space Mono',monospace;
  border-top:1px solid var(--bord);
  line-height:1.8;
}}

@media(max-width:900px){{
  .grid{{grid-template-columns:1fr;padding:12px}}
  .span-full,.span-half{{grid-column:1}}
  header,.kpi-row{{padding:12px 16px}}
}}
</style>
</head>
<body>

<header>
  <div class="logo">▶ YouTube</div>
  <h1>Trending <span>Analytics</span> Dashboard</h1>
  <div class="header-right">
    <span class="tag">100 videos · Top Global</span>
    <span class="tag tag-green">✓ Prefect 3 · Parte II</span>
    <span class="tag">Plotly 2.35</span>
  </div>
</header>

{kpi_html}

<div class="grid">
{charts_html}
{cat_table}
</div>

<footer>
  Fuente: Kaggle <code>datasnaek/youtube-new</code> &nbsp;·&nbsp;
  Pipeline orquestado con <b>Prefect 3</b> &nbsp;·&nbsp;
  Dashboard generado con <b>Plotly</b> + <b>Pandas</b><br>
  Parte I (ingesta &amp; transformación) → Parte II (visualización interactiva)
</footer>

</body>
</html>"""

    with open(out_path, "w", encoding="utf-8") as f:
        f.write(html)
    print(f"✅  Dashboard guardado → {out_path}")
    return out_path


# ═══════════════════════════════════════════════════════════════
#  FLOW PREFECT — Parte II
# ═══════════════════════════════════════════════════════════════
def dashboard_flow():
    """
    Flow de Prefect para la Parte II.
    Para usar con Prefect añade @flow y @task a cada función.
    """
    print("── [1/4] Cargando CSV procesado de la Parte I …")
    df = cargar_datos(PROCESSED_CSV)
    print(f"         {len(df)} filas · {df['country'].nunique()} países · {df['category_name'].nunique()} categorías")

    print("── [2/4] Cargando JSONs de categorías …")
    cat_json = cargar_categorias_json(RAW_DIR)
    # Enriquecer df con nombres de JSON
    df["category_name"] = df["category_id"].map(cat_json).fillna(df["category_name"])
    print(f"         {len(cat_json)} categorías cargadas desde JSON")

    print("── [3/4] Construyendo 8 figuras interactivas …")
    out = construir_dashboard(df, cat_json, OUTPUT_HTML)

    print(f"── [4/4] Listo → {out}")
    return out


if __name__ == "__main__":
    dashboard_flow()
