import streamlit as st
import pandas as pd
import plotly.express as px
import requests
import os

# Configuration de base de la page
st.set_page_config(page_title="Dataclysme Dashboard", layout="wide", page_icon="🌤️")

# Titre Principal
st.title("🌍 Dashboard Interactif : Dataclysme")
st.markdown("Visualisation des impacts météorologiques depuis l'API locale.")

# Configuration API (soit localhost, soit le nom du service docker si déployé avec compose)
API_URL = os.getenv("API_URL", "http://localhost:8000")

# --- Interface Latérale (Filtres) ---
st.sidebar.header("🔍 Filtres & Paramètres")

# Choix du Datamart
datamart = st.sidebar.radio(
    "1. Choisissez le domaine :",
    options=[("Risques Météorologiques", "risks"), 
             ("Impact Tourisme", "tourism"), 
             ("Impact Agriculture", "agriculture")],
    format_func=lambda x: x[0]
)[1]

# Récupération conditionnelle de l'année (on va d'abord requêter l'API pour voir les données, 
# mais l'API peut filtrer si on lui donne une année, ou on peut tout récupérer et filtrer ici).
# Pour ce dashboard, on va extraire les données
@st.cache_data(ttl=300)
def fetch_data(endpoint: str, limit: int = 150000):
    url = f"{API_URL}/{endpoint}?limit={limit}"
    try:
        # Augmentation du timeout à 120s car la requête BDD pour de gros volumes est lourde
        response = requests.get(url, timeout=120)
        response.raise_for_status()
        data = response.json()
        if not data:
            return pd.DataFrame()
        return pd.DataFrame(data)
    except Exception as e:
        st.error(f"Erreur de connexion à l'API: {e}")
        return pd.DataFrame()

# Chargement des données selon le datamart sélectionné
with st.spinner("Chargement des données depuis l'API..."):
    df = fetch_data(datamart)

if df.empty:
    st.warning("Aucune donnée n'a été retournée par l'API pour ce datamart.")
else:
    # Récupérer la liste des années pour le slider
    if 'year' in df.columns:
        df['year'] = df['year'].astype(int)
        min_year = int(df['year'].min())
        max_year = int(df['year'].max())
        
        selected_year = st.sidebar.slider(
            "2. Sélectionnez l'année :", 
            min_value=min_year, max_value=max_year, value=max_year
        )
        
        # Filtre local
        df_filtered = df[df['year'] == selected_year]
    else:
        df_filtered = df

    if df_filtered.empty:
        st.info("Aucune donnée disponible pour les filtres sélectionnés.")
    else:
        # Dictionnaire de configuration pour des légendes et couleurs adaptées
        METRICS_CONFIG = {
            "avg_temp_c": {"label": "Température Moyenne (°C)", "colorscale": "RdBu_r"},
            "min_temp_c": {"label": "Température Minimum (°C)", "colorscale": "RdBu_r"},
            "max_temp_c": {"label": "Température Maximum (°C)", "colorscale": "RdBu_r"},
            "precipitation_mm": {"label": "Précipitations (mm)", "colorscale": "Blues"},
            "snow_depth_mm": {"label": "Épaisseur de Neige (mm)", "colorscale": "PuBu"},
            "avg_wind_dir_deg": {"label": "Direction du Vent Moyenne (°)", "colorscale": "HSV"},
            "avg_wind_speed_kmh": {"label": "Vitesse du Vent Moyenne (km/h)", "colorscale": "Reds"},
            "peak_wind_gust_kmh": {"label": "Rafales de Vent Max (km/h)", "colorscale": "Reds"},
            "avg_sea_level_pres_hpa": {"label": "Pression Niveaux Mer (hPa)", "colorscale": "Viridis"},
            "sunshine_total_min": {"label": "Ensoleillement Total (min)", "colorscale": "YlOrRd"},
        }

        # Identification des métriques (colonnes hors index de base)
        base_cols = ['year', 'country', 'region', 'city_name']
        available_metrics = [c for c in df_filtered.columns if c not in base_cols]
        
        # Selectbox avec des labels propres (au lieu du nom de la colonne de la BDD)
        metric_labels = [METRICS_CONFIG.get(m, {}).get("label", m) for m in available_metrics]
        selected_label = st.sidebar.selectbox("3. Sélectionnez la métrique à observer :", metric_labels)
        
        # Retrouver la colonne technique correspondante
        selected_metric = next(m for m in available_metrics if METRICS_CONFIG.get(m, {}).get("label", m) == selected_label)
        metric_conf = METRICS_CONFIG.get(selected_metric, {"label": selected_metric, "colorscale": "Plasma"})
        
        # Préparation de l'agrégation nationale pour la carte
        df_country = df_filtered.groupby("country")[selected_metric].mean().reset_index()

        def get_arrow(deg, symbol_only=False):
            if pd.isna(deg): return ""
            idx = int(round((deg % 360) / 45.0)) % 8
            if symbol_only:
                return ["⬆️", "↗️", "➡️", "↘️", "⬇️", "↙️", "⬅️", "↖️"][idx]
            return ["⬆️ N", "↗️ NE", "➡️ E", "↘️ SE", "⬇️ S", "↙️ SW", "⬅️ W", "↖️ NW"][idx]

        # Application du nom de la légende (label)
        leg_title = metric_conf["label"]
        df_country.rename(columns={selected_metric: leg_title}, inplace=True)

        st.subheader(f"🗺️ Répartition Globale : `{leg_title}` en {selected_year if 'year' in df.columns else 'global'}")

        # Tracer la carte choroplèthe avec Palette Intelligente
        fig = px.choropleth(
            df_country,
            locations="country",
            locationmode="country names",
            color=leg_title,
            hover_name="country",
            color_continuous_scale=metric_conf["colorscale"],
            title=f"Moyennes Nationales : {leg_title}"
        )

        # Ajouter les flèches sur la carte principale pour le vent !
        if selected_metric == "avg_wind_dir_deg":
            df_country["Arrow"] = df_country[leg_title].apply(lambda x: get_arrow(x, symbol_only=True))
            fig.add_scattergeo(
                locations=df_country["country"],
                locationmode="country names",
                text=df_country["Arrow"],
                mode="text",
                textfont=dict(size=18, color="black"),
                showlegend=False,
                hoverinfo="skip"
            )

        fig.update_layout(
            margin={"r":0,"t":40,"l":0,"b":0},
            geo=dict(showcoastlines=True, coastlinecolor="Black"),
            height=600
        )
        st.plotly_chart(fig, use_container_width=True)

        # Graphique personnalisé si on regarde la direction du vent
        if selected_metric == "avg_wind_dir_deg":
            st.subheader(f"🧭 Boussole Mondiale : `{leg_title}`")
            if 'city_name' in df_filtered.columns:
                # Échantillon de 30 villes (un tri n'a pas de sens pour un angle)
                top_cities = df_filtered.dropna(subset=[selected_metric]).head(30).copy()
                top_cities["Direction"] = top_cities[selected_metric].apply(lambda x: get_arrow(x, symbol_only=False))
                top_cities["Label"] = top_cities["city_name"] + " " + top_cities["Direction"]
                
                fig_polar = px.scatter_polar(
                    top_cities,
                    r=[1]*len(top_cities),
                    theta=selected_metric,
                    color="country",
                    text="Label",
                    title=f"Répartition des vents - Échantillon de 30 Villes",
                    labels={selected_metric: leg_title},
                )
                fig_polar.update_traces(textposition='top center', marker=dict(size=12, symbol="circle"))
                fig_polar.update_layout(
                    height=600,
                    polar=dict(
                        angularaxis=dict(rotation=90, direction="clockwise"),
                        radialaxis=dict(visible=False, range=[0, 1.4])
                    )
                )
                st.plotly_chart(fig_polar, use_container_width=True)
        else:
            # Affichage d'un Top 15 Classique
            st.subheader(f"📊 Top 15 des villes sur le critère `{leg_title}`")
            if 'city_name' in df_filtered.columns:
                top_cities = df_filtered.nlargest(15, selected_metric)[['city_name', 'country', selected_metric]]
                fig_bar = px.bar(
                    top_cities, 
                    x='city_name', 
                    y=selected_metric, 
                    color="country",
                    title=f"Records Mondiaux - {leg_title}",
                    text_auto='.2s',
                    labels={selected_metric: leg_title, "city_name": "Villes", "country": "Pays"}
                )
                fig_bar.update_layout(xaxis_title="Villes", yaxis_title=leg_title)
                st.plotly_chart(fig_bar, use_container_width=True)
            
        # Affichage du jeu de données brut
        with st.expander("Consulter les données brutes"):
            st.dataframe(df_filtered)