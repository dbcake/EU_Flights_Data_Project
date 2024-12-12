import streamlit as st
import pydeck as pdk
import pandas as pd
import numpy as np
from math import floor
from typing import List
import branca.colormap as cm
import snowflake.snowpark as sp
from snowflake.snowpark.context import get_active_session

st.set_page_config(
    page_title="European Air Traffic Overview",
    page_icon="🌍",
    layout="wide",
    initial_sidebar_state="expanded",
)


def open_session():
    snow_session = None
    try:
        snow_session = get_active_session()
    except:
        creds = {
            "account": "aab46027.us-west-2",
            "user": "dataexpert_student",
            "password": "DataExpert123!",
            "database": "DATAEXPERT_STUDENT",
            "schema": "DBCAKE",
            "role": "ALL_USERS_ROLE",
            "warehouse": "COMPUTE_WH",
        }
        snow_session = sp.Session.builder.configs(creds).create()
    return snow_session


session = open_session()


def get_quantiles(df_column: pd.Series, quantiles: List) -> pd.Series:
    min = df_column.min()
    max = df_column.max()
    diff = max - min
    quantile_values = [int(min + q * diff) for q in quantiles]
    return pd.Series(quantile_values)


def get_color(
    df_column: pd.Series, colors: List, vmin: int, vmax: int, index: pd.Series
) -> pd.Series:
    color_map = cm.LinearColormap(colors, vmin=vmin, vmax=vmax, index=index)
    return df_column.apply(color_map.rgb_bytes_tuple)


@st.cache_data(ttl=1200)
def fetch_df(query):
    df = session.sql(query).to_pandas()
    return df


df = fetch_df("SELECT * FROM dbcake.sum_yearly_metric_array ORDER BY count DESC LIMIT 80000")


def map():
    VIEW_STATE = pdk.ViewState(
        latitude=47,
        longitude=5,
        zoom=3.7,
        max_zoom=15,
        pitch=30,
        bearing=-10,
        height=700,
        width=1000,
    )

    quantiles = get_quantiles(df["COUNT"], [0.001, 0.03, 0.08, 0.2, 0.35, 0.45, 1])
    colors = ["#444444", "#0198BD", "#49E3CE", "#D8FEB5", "#FEEDB1", "#FEAD54", "#D1374E"]
    df["COLOR"] = get_color(df["COUNT"], colors, quantiles.min(), quantiles.max(), quantiles)
    hexlayer = pdk.Layer(
        "H3HexagonLayer",
        df[df["COUNT"] > 0],
        get_hexagon="HEX",
        get_fill_color="COLOR",
        get_line_color="COLOR",
        get_elevation="COUNT",
        auto_highlight=True,
        elevation_scale=1,
        pickable=True,
        elevation_range=[0, 1],
        get_elevation_weight="COUNT",
        extruded=True,
        coverage=1,
        opacity=0.1,
    )
    map_style = "mapbox://styles/mapbox/dark-v10"
    r = pdk.Deck(
        layers=[hexlayer],
        map_style=map_style,
        initial_view_state=VIEW_STATE,
        width=1000,
        height=700,
    )
    with st.spinner("Drawing Map..."):
        st.pydeck_chart(r)


st.markdown("# 🌍 Air Traffic Overview")
st.write(
    """This Heatmap shows the air traffic hotspots in the European Airspace grouped into hexagonal bins. 
    """
)
map()
st.write(
    """
    The height of the column represents the distinct number of flights."""
)
