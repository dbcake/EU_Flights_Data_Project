import streamlit as st
import pydeck as pdk
import pandas as pd
import numpy as np
from math import floor
from typing import List
import branca.colormap as cm
import snowflake.snowpark as sp
from snowflake.snowpark.context import get_active_session
import matplotlib.pyplot

st.set_page_config(
    page_title="Airport Noise Pollution Data",
    page_icon="🔊",
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


@st.cache_data(ttl=1200)
def sp_to_df(procedure, **kwargs):
    args = list(kwargs.values())
    result = session.call(procedure, *args)
    df = result.to_pandas()
    df["HEX"] = df["HEX"].apply(lambda x: x.replace('"', ""))
    return df


if "mode" not in st.session_state:
    st.session_state.mode = "default"
if "lat" not in st.session_state:
    st.session_state.lat = 47
if "lon" not in st.session_state:
    st.session_state.lon = 5
if "h3_resolution" not in st.session_state:
    st.session_state.h3_resolution = 8
if "zoom" not in st.session_state:
    st.session_state.zoom = 3.5
if "is_expanded" not in st.session_state:
    st.session_state["is_expanded"] = True


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


ap_df = fetch_df(
    "SELECT airport, icao_code, type, country, latitude, longitude FROM dbcake.dim_airports WHERE dbt_valid_to IS NULL"
)


def map():
    VIEW_STATE = pdk.ViewState(
        latitude=st.session_state.lat,
        longitude=st.session_state.lon,
        zoom=10,
        max_zoom=15,
        pitch=30,
        bearing=-10,
        height=700,
        width=1000,
    )
    quantiles = get_quantiles(df["COUNT"], [0.01, 0.05, 0.1, 0.25, 0.4, 0.5, 1])
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
        opacity=0.05,
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


st.markdown("# 🔊 Airport Noise Pollution Data")
st.sidebar.header("Select Airport")


container = st.expander("Search by Airport", expanded=st.session_state["is_expanded"])

st.write(
    """Select a Country and an Airport to see the flight traffic in the vicinity as hexagonal bins.
    """
)
draw_map = False
with st.sidebar:
    h3_resolution = st.slider(
        "🔍 Set resolution for the hexagonal bins",
        min_value=6,
        max_value=10,
        value=8,
        key="w_h3_resolution",
    )
    st.write("""Higher number means a higher resolution.""")
    sel_country = st.selectbox(
        "🏳️ Filter Country ",
        np.sort(ap_df["COUNTRY"].unique()),
        index=None,
        key="w_country",
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
    if sel_country is not None:
        sel_airport = st.selectbox(
            "🛃 Select Airport ",
            (
                ap_df[(ap_df["COUNTRY"] == sel_country) & (ap_df["TYPE"].isin(sel_airport_type))][
                    "AIRPORT"
                ].unique()
            ),
            index=None,
            placeholder="None selected",
            key="w_airport",
        )
        if sel_airport is not None:
            airport_dict = ap_df[ap_df["AIRPORT"] == sel_airport].to_dict(orient="records")[0]
            st.session_state.lon = airport_dict["LONGITUDE"]
            st.session_state.lat = airport_dict["LATITUDE"]
            st.session_state.icao_code = airport_dict["ICAO_CODE"]
            st.session_state.mode = "airport"
            st.session_state.sel_country = sel_country
            st.session_state.sel_airport = sel_airport
            st.session_state.h3_resolution = h3_resolution
            draw_map = True

if draw_map:
    try:
        kwargs = {
            "lat": st.session_state.lat,
            "lon": st.session_state.lon,
            "res": st.session_state.h3_resolution,
        }
        df = sp_to_df("dbcake.AGG_BY_COORDINATES", **kwargs)
        with st.spinner("Drawing Map..."):
            map()
        try:
            hour_df = fetch_df(
                f"""
                SELECT 
                    HOUR(e.event_time) AS hour
                    ,COUNT(DISTINCT flight_id ) AS count
                FROM fct_flights f
                JOIN fct_events e
                ON f.id=e.flight_id
                WHERE (f.ADEP = '{st.session_state.icao_code}' AND e.type IN ('take-off'))
                OR (f.ADES='{st.session_state.icao_code}' AND e.type IN ('landing') )
                GROUP BY HOUR(e.event_time)
                ORDER BY HOUR(e.event_time)
                """
            )
            st.markdown("---")
            st.markdown("### Hourly Traffic Distribution")
            st.bar_chart(hour_df, x="HOUR", y="COUNT")
            ac_df = fetch_df(
                f"""
                SELECT 
                    a.enginetype
                    ,a.enginecount
                    ,a.manufacturericao
                    ,a.wtc
                    ,a.typedesignator
                    ,count( DISTINCT f.id) AS count
                FROM fct_flights f
                JOIN dim_aircrafts a
                ON f.icao24 = a.icao24
                WHERE (f.ADEP = '{st.session_state.icao_code}' OR f.ADES='{st.session_state.icao_code}' ) AND a.typedesignator IS NOT NULL
                GROUP BY 1,2,3,4,5
                ORDER BY count( DISTINCT f.id) DESC
                LIMIT 10
                """
            )
            st.markdown("### Aircraft Manufacturers")
            st.bar_chart(
                ac_df,
                x="MANUFACTURERICAO",
                y="COUNT",
            )
            st.markdown("### Aircraft Types")
            st.bar_chart(
                ac_df,
                x="TYPEDESIGNATOR",
                y="COUNT",
            )
            op_df = fetch_df(
                f"""
                SELECT 
                    a.operatoricao
                    ,count( DISTINCT f.id) AS count
                FROM fct_flights f
                JOIN dim_aircrafts a
                ON f.icao24 = a.icao24
                WHERE (f.ADEP = '{st.session_state.icao_code}' OR f.ADES='{st.session_state.icao_code}' ) AND a.operatoricao <> ''
                GROUP BY 1
                ORDER BY count( DISTINCT f.id) DESC
                LIMIT 10
                """
            )
            st.markdown("### Aircraft Operators")
            st.bar_chart(
                op_df,
                x="OPERATORICAO",
                y="COUNT",
            )
        except:
            st.error("Error displaying charts")
    except:
        st.error("👈 Please choose an airport through the filters in the sidebar.")
