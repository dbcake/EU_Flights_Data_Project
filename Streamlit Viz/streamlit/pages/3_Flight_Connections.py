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
    page_title="Busiest Connections",
    page_icon="🔗",
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


query = """
WITH airport_pairs AS ( 
    SELECT 
        ADEP
        ,ADES
        ,COUNT(ADEP) count
    FROM fct_flights
    WHERE ADEP IS NOT NULL AND ADES IS NOT NULL
    GROUP BY ADEP, ADES
    ORDER BY COUNT(ADEP) DESC
)
SELECT 
    a.*
    ,d1.latitude AS adep_lat
    ,d1.longitude AS adep_lon
    ,d2.latitude AS ades_lat
    ,d2.longitude AS ades_lon
    ,d1.airport AS adep_airport
    ,d2.airport AS ades_airport
    ,d1.country AS adep_country
    ,d2.country AS ades_country
    ,d1.municipality AS adep_mun
    ,d2.municipality AS ades_mun
    ,d1.type AS adep_type
    ,d2.type AS ades_type
FROM airport_pairs a
JOIN dim_airports d1 
ON a.adep = d1.icao_code AND d1.dbt_valid_to IS NULL
JOIN dim_airports d2
ON a.ades = d2.icao_code AND d2.dbt_valid_to IS NULL
WHERE count > 10
ORDER BY count DESC
"""
df = fetch_df(query)


def map():
    VIEW_STATE = pdk.ViewState(
        latitude=47,
        longitude=11,
        zoom=3.7,
        max_zoom=15,
        pitch=30,
        bearing=-10,
        height=700,
        width=1000,
    )
    # map_style = "https://basemaps.cartocdn.com/gl/dark-matter-nolabels-gl-style/style.json"
    map_style = "mapbox://styles/mapbox/dark-v10"
    # map_style = "mapbox://styles/mapbox/light-v10"
    map_df = df[
        (df["COUNT"] >= monthly_flight_count[0])
        & (df["COUNT"] <= monthly_flight_count[1])
        & (df["ADEP_TYPE"].isin(sel_airport_type))
        & (df["ADES_TYPE"].isin(sel_airport_type))
        & (df["ADEP_COUNTRY"].isin(sel_origin_country))
        & (df["ADES_COUNTRY"].isin(sel_destination_country))
    ]
    layer = pdk.Layer(
        "GreatCircleLayer",
        map_df,
        pickable=True,
        get_stroke_width=1,
        get_source_position=["ADEP_LON", "ADEP_LAT"],
        get_target_position=["ADES_LON", "ADES_LAT"],
        get_source_color=[64, 255, 0],
        get_target_color=[0, 128, 200],
        auto_highlight=True,
    )
    airport_names = (
        pdk.Layer(
            "TextLayer",
            ap_df[
                ap_df["AIRPORT"].isin(map_df["ADEP_AIRPORT"].unique())
                | ap_df["AIRPORT"].isin(map_df["ADES_AIRPORT"].unique())
            ],
            get_position=["LONGITUDE", "LATITUDE"],
            get_text="MUNICIPALITY",
            get_color=[128, 128, 128, 1000],
            get_size=12,
            get_alignment_baseline="'bottom'",
        ),
    )
    r = pdk.Deck(
        layers=[layer],
        map_style=map_style,
        initial_view_state=VIEW_STATE,
        width=1000,
        height=700,
    )

    with st.spinner("Drawing Map..."):
        st.pydeck_chart(r)


st.markdown("# 🔗 Flight Connections")
st.sidebar.header("Flight Connections")
st.write(
    """This map shows the busiest Flight Connections between Countries. 
    """
)


ap_df = fetch_df(
    "SELECT airport, municipality, type, country, latitude, longitude FROM dbcake.dim_airports WHERE dbt_valid_to IS NULL"
)

with st.sidebar:
    monthly_flight_count = st.slider(
        "🧮 Number of Flights in the Time Period",
        min_value=10,
        max_value=300,
        value=(30, 250),
        key="w_monthly_flight_count",
    )

    sel_origin_country = st.multiselect(
        "🛫 Filter by Origin Country ",
        np.sort(ap_df["COUNTRY"].unique()),
        [
            "Spain",
            "Portugal",
            "Poland",
            "Germany",
            "Italy",
            "United Kingdom",
            "Ireland",
            "Austria",
        ],
        key="w_origin_country",
    )
    sel_destination_country = st.multiselect(
        "🛬 Filter by Destination Country",
        np.sort(ap_df["COUNTRY"].unique()),
        [
            "Belgium",
        ],
        key="w_destination_country",
    )

    option_map = {
        "large_airport": "Large",
        "medium_airport": "Medium",
        "small_airport": "Small",
    }
    sel_airport_type = st.multiselect(
        "📏 Airport size ",
        option_map.keys(),
        default="large_airport",
        format_func=lambda option: option_map[option],
        key="w_airport_type",
    )


map()
st.write(
    """ Filter in the sidebar for the number of flights per time period and origin/destination countries or airport types."""
)
