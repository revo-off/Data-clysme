import requests
import pandas as pd
import plotly.express as px
import streamlit as st


st.set_page_config(page_title="Dataclysme Dashboard", layout="wide")
st.title("Dataclysme - Visualisation Datamarts")

api_base = st.sidebar.text_input("API base URL", "http://localhost:8000")
username = st.sidebar.text_input("Username", "admin")
password = st.sidebar.text_input("Password", "admin123", type="password")
page_size = st.sidebar.slider("Rows per datamart", 100, 5000, 1000, 100)


def fetch_token(base_url, user, pwd):
    resp = requests.post(
        f"{base_url}/auth/login",
        json={"username": user, "password": pwd},
        timeout=30,
    )
    resp.raise_for_status()
    return resp.json()["access_token"]


def fetch_datamart(base_url, token, name, size):
    headers = {"Authorization": f"Bearer {token}"}
    resp = requests.get(
        f"{base_url}/api/v1/datamarts/{name}",
        params={"page": 1, "page_size": size},
        headers=headers,
        timeout=60,
    )
    resp.raise_for_status()
    payload = resp.json()
    return pd.DataFrame(payload["data"]), payload["pagination"]


if st.sidebar.button("Load datamarts"):
    try:
        token = fetch_token(api_base, username, password)
        df_risks, pg_risks = fetch_datamart(api_base, token, "risks", page_size)
        df_tourism, pg_tourism = fetch_datamart(api_base, token, "tourism", page_size)
        df_agri, pg_agri = fetch_datamart(api_base, token, "agriculture", page_size)

        st.success("Datamarts loaded from API")

        c1, c2, c3 = st.columns(3)
        c1.metric("Risks rows", pg_risks["total_rows"])
        c2.metric("Tourism rows", pg_tourism["total_rows"])
        c3.metric("Agriculture rows", pg_agri["total_rows"])

        st.subheader("Graph 1 - Avg temperature by month (Risks)")
        if not df_risks.empty and {"month", "avg_temp_c"}.issubset(df_risks.columns):
            d1 = df_risks.groupby("month", as_index=False)["avg_temp_c"].mean().sort_values("month")
            fig1 = px.line(d1, x="month", y="avg_temp_c", markers=True)
            st.plotly_chart(fig1, use_container_width=True)
        else:
            st.info("Risks datamart is empty or missing columns.")

        st.subheader("Graph 2 - Total precipitation by month (Tourism)")
        if not df_tourism.empty and {"month", "precipitation_mm"}.issubset(df_tourism.columns):
            d2 = df_tourism.groupby("month", as_index=False)["precipitation_mm"].sum().sort_values("month")
            fig2 = px.bar(d2, x="month", y="precipitation_mm")
            st.plotly_chart(fig2, use_container_width=True)
        else:
            st.info("Tourism datamart is empty or missing columns.")

        st.subheader("Graph 3 - Temperature distribution (Agriculture)")
        if not df_agri.empty and "avg_temp_c" in df_agri.columns:
            fig3 = px.histogram(df_agri, x="avg_temp_c", nbins=40)
            st.plotly_chart(fig3, use_container_width=True)
        else:
            st.info("Agriculture datamart is empty or missing columns.")

    except Exception as exc:
        st.error(f"Error while loading API data: {exc}")
