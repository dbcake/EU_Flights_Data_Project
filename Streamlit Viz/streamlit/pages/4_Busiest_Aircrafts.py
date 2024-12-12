import streamlit as st
import pydeck as pdk
import pandas as pd
import numpy as np
from math import floor
from typing import List
import branca.colormap as cm
import snowflake.snowpark as sp
from snowflake.snowpark.context import get_active_session
from datetime import datetime

st.set_page_config(
    page_title="Busiest Aircrafts",
    page_icon="🐝",
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
WITH top_10_busiest AS(
    SELECT 
        icao24
        ,count(id) as flight_count
    FROM fct_flights
    WHERE DOF BETWEEN '2024-01-01' AND '2024-02-01'
    AND ADEP IS NOT NULL AND ADES IS NOT NULL
    GROUP BY icao24
    ORDER BY count(id) DESC
    LIMIT 10
) ,flight_ids AS (
SELECT 
    id 
FROM fct_flights 
WHERE ICAO24 in ( SELECT icao24 FROM top_10_busiest )
AND ADEP IS NOT NULL AND ADES IS NOT NULL
)
SELECT 
    f.icao24
    ,f.adep
    ,f.ades
    ,f.dof
    ,e.type
    ,e.event_time
    ,e.longitude
    ,e.latitude
    ,e.altitude
    ,e.time_passed
    ,e.distance_flown
FROM fct_events e
JOIN fct_flights f
ON e.flight_id = f.id
WHERE flight_id IN (SELECT id from flight_ids)
ORDER BY f.icao24, e.event_time
"""
df = fetch_df(query)

st.sidebar.header("🐝 Busiest Aircrafts Filters")
with st.sidebar:
    date_of_flight = st.slider(
        "🧮 Day of Flight ",
        min_value=datetime(2024, 1, 1),
        max_value=datetime(2024, 1, 31),
        value=(datetime(2024, 1, 1), datetime(2024, 1, 10)),
        format="D",
        key="w_date_of_flight",
    )

    sel_icao24 = st.multiselect(
        "🛫 Filter by Aircraft Identification Code ",
        np.sort(df["ICAO24"].unique()),
        np.sort(df["ICAO24"].unique()[:6:2]),
        key="w_icao24",
    )

map_df = df[(df["ICAO24"].isin(sel_icao24))]
map_df = map_df[
    (map_df["DOF"] >= date_of_flight[0].date()) & (map_df["DOF"] <= date_of_flight[1].date())
]

map_df.dropna(subset=["ALTITUDE"], inplace=True)
quantiles = get_quantiles(map_df["ALTITUDE"], [0.01, 0.05, 0.15, 0.3, 0.5, 0.7, 1])
colors = ["#444444", "#0198BD", "#49E3CE", "#D8FEB5", "#FEEDB1", "#FEAD54", "#D1374E"]
map_df["COLOR"] = get_color(
    map_df["ALTITUDE"], colors, quantiles.min(), quantiles.max(), quantiles
)

map_df = (
    map_df.groupby(["ICAO24", "DOF", "ADEP", "ADES"])
    .apply(
        lambda x: [
            list(x["LONGITUDE"]),
            list(x["LATITUDE"]),
            list(x["TIME_PASSED"]),
            list(x["ALTITUDE"]),
            list(x["COLOR"]),
        ]
    )
    .apply(pd.Series)
)

map_df.rename(
    columns={0: "LONGITUDE", 1: "LATITUDE", 2: "TIME_PASSED", 3: "ALTITUDE", 4: "COLOR"},
    inplace=True,
)
map_df["COORDINATES"] = map_df.apply(lambda x: list(zip(x["LONGITUDE"], x["LATITUDE"])), axis=1)


def map():

    VIEW_STATE = pdk.ViewState(
        latitude=47,
        longitude=5,
        zoom=3.5,
        max_zoom=15,
        pitch=30,
        bearing=-10,
        height=700,
        width=1000,
    )
    map_style = "mapbox://styles/mapbox/dark-v10"

    layer = pdk.Layer(
        "TripsLayer",
        map_df,
        get_path="COORDINATES",
        get_timestamps="TIME_PASSED",
        get_color="COLOR",
        opacity=0.5,
        width_min_pixels=3,
        rounded=True,
        current_time=10000,
        fade_trail=False,
        cap_rounded=True,
        joint_rounded=True,
    )

    airport_names = (
        pdk.Layer(
            "TextLayer",
            ap_df[
                ap_df["ICAO_CODE"].isin(map_df.index.unique("ADEP"))
                | ap_df["ICAO_CODE"].isin(map_df.index.unique("ADEP"))
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


st.markdown("## 🐝 Busiest Aircrafts")

st.markdown(
    """This map shows the routes of the TOP 10 aircraft with the highest number of flights in the period. 
    """
)


ap_df = fetch_df(
    "SELECT airport, icao_code, municipality, type, country, latitude, longitude FROM dbcake.dim_airports WHERE dbt_valid_to IS NULL"
)


map()
st.markdown(
    """
    The color of the line shows the altitude of the aircraft.
    """
)
