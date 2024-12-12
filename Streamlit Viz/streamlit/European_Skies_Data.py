import streamlit as st
import snowflake.snowpark as sp
from snowflake.snowpark.context import get_active_session

st.set_page_config(
    page_title="Analytics Engineering Capstone Project.",
    page_icon="🎓",
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

st.markdown("# European Skies Data 🛫")


st.write("A Capstone Project for the DataExpert.io Analytics Engineering Bootcamp.")

st.sidebar.success("Explore the data in the pages above. ☝️")

tab1, tab2, tab3, tab4, tab5, tab6, tab7, tab8 = st.tabs(
    [
        "Motivation",
        "Steps",
        "Data Sources",
        "EDA",
        "Data Model",
        "Tech Stack",
        "Pipeline",
        "Conclusion",
    ]
)

with tab1:
    st.header("The Motivation")
    st.markdown(
        """
        #### Personal Motivation
        I have always lived under a flight corridor, and I thought I was used to the noise pollution, but my current neighborhood is severely impacted. The noise from the aircraft affects my personal life, health, and work. Using this app, individuals who are sensitive to noise pollution can easily look up the city and neighborhood they are interested in before deciding to rent a flat, buy a new home, or organize an important event.
        """
    )
    st.markdown(
        """
        #### The Problem
        The European airspace faces several significant challenges, with high levels of traffic congestion, flight delays, and increasing environmental concerns such as noise pollution. 
        - **Noise Pollution**: As air traffic volumes increase, noise pollution has become a growing issue, especially for communities near airports.
        - **Congestion and Capacity Limits**: European airspace is one of the busiest globally, with peak times often leading to bottlenecks. 
        #### The Goal
        The goal of the project is to analyze open sourced data to provide insights for the above issues.
        1. Noise Pollution Analysis to identify areas experiencing high traffic and high noise levels. 
        2. This data could also feed predictive models to anticipate congestions and delays.
        The primary output of the project is a **visualization that enables the public to identify areas** of low and high noise pollution due to air traffic.
        #### Target Audience 
        - **Individuals** can use this data to assess exposure to flight traffic related noise pollution for any areas of interest e.g. before making a decision to move to a new home.
        - **Local Communities affected by noise pollution** can advocate for noise mitigation policies and engage in constructive discussions with decision makers.
        - **Air Traffic Management (ATM) Agencies** could use these insights to explore alternative paths for flights to mitigate noise pollution, refine air traffic management and reduce delays.
        """
    )


with tab2:
    st.header("The Steps")
    st.markdown(
        """
        This is a rough list of the steps I went through building this project.
        #### 1. Researching available Data Sources
        - Given the requirements of the Capstone Project I started with static and dynamic data sources shared in the Bootcamp
        - Searched the internet for available free and paid data sources, APIs that are interesting
        #### 2. Evaluated most interesting Data Sources through below factors:
        - The availlability of data, e.g. API limitations
        - The format of the data, e.g. what file format
        - The quantity of data
        - The price of data
        - Concluded on the data sources used and the use case
        #### 3. Performed Exploratory Data Analysis
        - I chosed Google Colab's Jupyter Notebook environment to experiment with the data and share it with others
        - I used DuckDB to easily process the files and run sum basic statistics
        - Identified Data Quality issues, e.g. missing data mostly in the flights and aircrafts data sources
        - I used Leafmap within the notebook to visualize Geospatial data as a proof of concept
        #### 4. Conceptual Data Model and Architecture Diagram
        - Created the Conceptual Data Model
        - Created an initial archictural overview based on the array of tools and services available to us within the Bootcamp
        #### 5. Created a Data Dictionary
        - From the Conceptual Data Model I created the Logical Data Model
        - After some experimentation with different tools I decided on the final tech stack
        - Created the Physical Data Model
        - Created a Data Dictionary listing all tables and fields with their information
        #### 6. Wrote the ETL pipelines to transform the data.
        - Created extract tasks to read parquet files from the web and load it into the Data Warehouse
        - Created transformation models in dbt 
        - Defined tests for each source and table
        #### 7. Ran the ETL pipelines and QA checks
        - Created dags for data sources of varying extract intervals
        - Tested and optimized the running of the dags
        #### 8. Created Visualizations
        - Built Streamlit app with custom visualizations and interactive widgets
        - Tested and improved user experience
        - Added instructions and contextual information for end users
    """
    )
with tab3:
    st.header("The Datasources")
    st.markdown(
        """
    #### Shortlist of Data sources and their types
    1. OPDI Fligths data from OpenSky Network in parquet files
    2. Airport database from OurAirports.com in CSV files
    3. Aircraft database from OpenSky Network in CSV files
    4. Aircraft type data from OpenSky Network in CSV file
    5. Country data from OurAirports.com in CSV file
    ---
    #### Flight Data from the Open Performance Data Initiative
    ###### About the Open Performance Data Initiative (OPDI) dataset
    This flight dataset is published under the **[Open Performance Data Initiative (OPDI)](https://www.opdi.aero/)** as sponsored by the **[Performance Review Commission](https://ansperformance.eu/about/prc/)** and in collaboration with the **[OpenSky Network (OSN)](https://opensky-network.org/)**.

    ###### Comparison of sources

    | API                                                                                                                  | Hist Pos data | Live Pos data | Hist schedule | Live schedule | Static data | Hist Price data | Pricing                           |
    | -------------------------------------------------------------------------------------------------------------------- | ------------- | ------------- | ------------- | ------------- | ----------- | --------------- | --------------------------------- |
    | [FR24](https://fr24api.flightradar24.com/docs#general-info)                                                          | Yes           | Yes           | Yes           | Yes           | Yes         | No              | $ 90 / month                      |
    | [Aviation Edge](https://aviation-edge.com/developers/#intro)                                                         | No            | Yes           | Yes           | Yes           | Yes         | No              | first $7/m, $300/m, 30000 call    |
    | [Aviationstack](https://aviationstack.com/documentation)                                                             | No            | No            | Yes           | Yes           | Yes         | No              | 100 call free, $50/m              |
    | [Amadeus Airfare prices](https://developers.amadeus.com/self-service/category/flights/api-doc/flight-price-analysis) | No            | No            | No            | No            | No          | Yes             | first 10000 call free, 0,0025 EUR |
    | [OPDI](https://www.opdi.aero/)                                                                                       | Yes*          | No            | Yes           | No            | Yes         | No              | Free                              |
    
    ###### Why I chose this data:
    It is the most complete flights dataset which is freely available.
    - Covers a period of 30 months
    - 30+ million flights
    - Free to use
    - Data is in a series of parquet files
    ---
    #### Airports and Runway Data by OurAirports
    ###### Why I chose this data:
    - Most comprehensive dataset around airports
    - Includes a table for countries and runways as well
    - Free

    ###### Comparison of sources

    | Name                                                                             | Up to date | Runway data | Country | Downloadable | Free |
    | -------------------------------------------------------------------------------- | ---------- | ----------- | ------- | ------------ | ---- |
    | [AirportDatabase](https://airportdatabase.net/)                                  | Yes        | No          | Yes     | No           | Yes  |
    | [OurAirports](https://ourairports.com/data/)                                     | Yes        | Yes         | Yes     | Yes          | Yes  |
    | [Global Airport Database](https://www.partow.net/miscellaneous/airportdatabase/) | Yes        | No          | Yes     | Yes          | Yes  |
    ---
    #### Aircrafts Data by  OpenSky Network
    ###### Why I chose this data:
    - Most comprehensive dataset of aircrafts
    - Has airline information
    - Free
    ###### Comparison of sources

    | Name                                                 | Up to date | Types | Manuf. | Airline | Country | File | Format | Free |
    | ---------------------------------------------------- | ---------- | ----- | ------ | ------- | ------- | ---- | ------ | ---- |
    | [OSN](https://opensky-network.org/aircraft-database) | Yes        | Yes   | Yes    | Yes     | Yes     | Yes  | .csv   | Yes  |
    | [Airframes.org](https://www.airframes.org/)          | Yes        | Yes   | Yes    | No      | No      | No   | N/A    | Yes  |
    | [Airfleets.net](https://www.airfleets.net/)          | Yes        | Yes   | Yes    | Yes     | Yes     | Yes  | .xls   | No   |
    
    """
    )
with tab6:
    st.header("The Tech Stack")
    st.markdown(
        """
        Analyzing such large dataset requires a robust and scalable data pipeline. This tech stack was chosen to handle high data volume, complex transformations, and deliver insights efficiently while ensuring scalability and flexibility. 
        Below is the description of each component and why it was chosen.
                
        ### High Level Architecture
        """
    )
    image8 = session.file.get_stream(
        '@"DATAEXPERT_STUDENT"."DBCAKE"."MLEY6DHY93REILXI (Stage)"/assets/ArchV3.png',
        decompress=False,
    ).read()
    st.image(image8, width=700)

    st.markdown(
        """
        #### Snowflake
        Centralized storage for all data, allowing performant querying and integration with other tools.
        ###### Why Snowflake?
        - Highly scalable data warehouse that handles large datasets efficiently
        - Supports Parquet and CSV file ingestion with custom file format definitions
        - Has general availability of H3 functions within Snowflake's SQL scripting
        #### Snowpark
        Used for ingesting data into Snowflake
        ###### Why Snowpark?
        - Provides an easy solution to ingest data into Snowflake
        - Enables a wide range of operations to complement SQL
        #### dbt
        To define and orchestrate transformations on the raw data and create aggregations.
        ###### Why dbt?
        - Simple to create and manage SQL-based transformations.
        - Provides version control, documentation, and testing for data pipelines.
        - Ensures modular and reusable transformation logic
        #### Airflow
        To Schedule and orchestrate ingestion, transformation, and analytics workflows.
        ###### Why Airflow?
        - Airflow allows scheduling and managing workflows with clear dependencies between tasks.
        - Can be used with dbt through Cosmos
        #### Cosmos
        Bridges Airflow orchestration with dbt's transformation logic, creating a unified ETL pipeline.
        ###### Why Cosmos?
        - Integrates dbt with Airflow
        - Executes dbt models as part of the Airflow pipeline
        #### Streamlit
        To build an interactive dashboard for visualizing data.
        ###### Why Streamlit?
        - Simplifies sharing insights with stakeholders via a web-based interface
        - Uses Python
        - Built into Snowflake
        #### DuckDB
        To Conduct exploratory analysis and testing of queries before creating them in Snowflake.
        ###### Why DuckDB?
        - Lightweight and fast for prototyping
        - Ideal for quickly exploring and querying Parquet and CSV files without loading the data into a warehouse
        

    """
    )
with tab5:
    st.header("The Data Model")
    st.markdown(
        """
        #### Conceptual Data Model
        """
    )
    image1 = session.file.get_stream(
        '@"DATAEXPERT_STUDENT"."DBCAKE"."MLEY6DHY93REILXI (Stage)"/assets/CDM.png',
        decompress=False,
    ).read()
    st.image(image1, width=700)
    st.markdown(
        """
        [Link](https://lucid.app/lucidchart/43e080e7-2bb1-4b75-b675-39dd35f028bb/edit?viewport_loc=329%2C38%2C2047%2C835%2C0_0&invitationId=inv_39afcf60-6186-434d-835e-ca2bcf9112d3)            
        """
    )
    st.markdown(
        """
        #### Logical Data Model
        """
    )
    image2 = session.file.get_stream(
        '@"DATAEXPERT_STUDENT"."DBCAKE"."MLEY6DHY93REILXI (Stage)"/assets/LDM.png',
        decompress=False,
    ).read()
    st.image(image2, width=700)
    st.markdown(
        """
        [Link](https://lucid.app/lucidchart/c255de4d-f613-4381-b961-141625ff38da/edit?viewport_loc=157%2C-75%2C1279%2C585%2C0_0&invitationId=inv_cfdb4266-f7d1-47b0-b1e5-35893c7988fc)
        """
    )
    st.markdown(
        """
        #### Physical Data Model
        """
    )
    image3 = session.file.get_stream(
        '@"DATAEXPERT_STUDENT"."DBCAKE"."MLEY6DHY93REILXI (Stage)"/assets/PDM.png',
        decompress=False,
    ).read()
    st.image(image3, width=700)
    st.markdown(
        """
        [Link](https://lucid.app/lucidchart/b515820a-61fd-445d-addc-7e89e08ea482/edit?viewport_loc=-188%2C257%2C1705%2C780%2C0_0&invitationId=inv_c8c78d4a-8932-43d4-97a8-449e0833be00)            
        """
    )

with tab4:
    st.header("The Data Exploration")

    st.markdown(
        """
        #### The goal of the EDA was to:
        - Understand the data structure
        - Identify patterns and anomalies in the data
        - Understand what data is relevant for the use case
        #### Tools used:
        - Google Colab, a cloud-based Python environment ideal for handling large datasets
        - DuckDB, an in-process SQL database for querying large-scale datasets efficiently
        - Matplotlob, Seaborn for creating plots and charts
        - Leafmap and Geopandas with h3 for geo data visualization
                """
    )
    st.markdown(
        """
        #### Setup Process:
        - First mounted a drive in the Google Drive storage
        - Added the files to the drive    
        - Loaded the files into DuckDB and created tables
        #### Writing queries
        - Queried the tables with SQL
        - Saved the query results in Pandas dataframes
        #### Data Cleaning
        - Checked data types to ensure consistency in temporal and numerical fields
        - Identified missing data, incomplete records
        - Checked for duplicates
        #### Visual Analysis
        - Created plots and charts for visually exploring the data
        - Created Geospatial visualizations to understand how the data could be used
        """
    )
    st.markdown(
        """
        [Link to the Notebook](https://colab.research.google.com/drive/1IsFWsnMqP4pvMb2yWFjPfHHJ_kCYxP47?usp=sharing)
        """
    )
with tab7:
    st.header("The Pipeline")
    st.markdown(
        """
        The pipeline uses modern data engineering practices to extract, clean, and model data using a Medallion architecture that includes Staging, Intermediate, and Datamart layers. 
        
        #### Data sources
        - Some of the data sources comprise of several parquet files broken down to an interval of either a month or 10 days
        - Other sources are CSV files, updated at a daily or monthly cadence.
        - All sources are available for download from different websites.
        - At the moment there is no live API available to get the same complete dataset so I decided the consume the available sources in this project with a time delay, starting from the beginning of 2024

        #### Extracting the Data
        - The most efficient way I found to ingest the data was to upload to Snowflake's internal stage and copy into tables with Snowflake's Snowpark API
        - I opted for this solution also because it will be easy to adjust when the dataset is migrated to an S3 bucket in the future

        ###### Tasks:
        - Python script to get the different files for each time period and upload it into a Snowflake stage
        - Using the Snowpark API to upload the file to an internal stage within Snowflake
        - Running a Snowflake query to copy the data from the stage into a newly created table
        - A Python script to clean up temporary files
        - Running a Snowflake query for basic Data Quality check
        - If it passes the next task deletes the file from the temporary stage
        #### Transformation with dbt
        The transformation process follows the Medallion Architecture:
        ###### 1. Staging Layer
        - Data is ingested "as-is," retaining all fields and formats to ensure data traceability.
        - Light transformations, type casting, renaming fields.
        ###### 2. Intermediate Layer 
        - Perform data enrichment, cleaning, and transformations.
        - Joining different datasets. 
        ###### 3. Datamart Layer
        - Analysis-ready datasets for the dashboard.
        - Creating aggregations and final metrics.
        
        #### Data Quality Checks in dbt
        All dbt models have some kind of testing included:
        - Built in Generic Tests, e.g.:
            - unique
            - not_null
            - accepted_values
        - Tests by dbt expectations, e.g.:
            - expect_column_values_to_be_between
            - expect_column_values_to_be_of_type
        
        #### Documentation 
        I used dbt's built in commands to generate documentation and create a static website with an overview of the models.

            dbt docs generate
            dbt docs serve
        
        #### Orchestration by Airflow 
        In the end I grouped the tasks into task groups and created two dags running at different frequencies:
        ###### Monthly Dag:
        - Monthly Flights data
        - Aircrafts data
        - Aircraft types data
        """
    )
    image9 = session.file.get_stream(
        '@"DATAEXPERT_STUDENT"."DBCAKE"."MLEY6DHY93REILXI (Stage)"/assets/DagM.png',
        decompress=False,
    ).read()
    st.image(image9, width=700)
    st.markdown(
        """
        ###### Weekly Dag:
        - Flight Events data
        - Flight Measurements data
        - Airports data
        """
    )
    image10 = session.file.get_stream(
        '@"DATAEXPERT_STUDENT"."DBCAKE"."MLEY6DHY93REILXI (Stage)"/assets/DagW.png',
        decompress=False,
    ).read()
    st.image(image10, width=700)

with tab8:
    st.header("The Conclusion")
    st.markdown(
        """
        This capstone project for the Analytics Engineering Bootcamp was a challenging experience, but I learned a lot.
        I had to design and implement a complex data pipeline, integrating several data sources and utilizing various modern data engineering tools like: 
        - Python
        - Trino
        - Spark
        - Snowflake
        - dbt
        - Airflow
        - Streamlit
        
        The main highlights for me working on the project:
        - Used some data sets with many millions of rows each
        - Doing EDA in **Google Colab** with **DuckDB**
        - Created an **SCD** table with **dbt Snapshot** functionality
        - Included a **reduced fact table** in the data model using complex data structure in the row
        - Adding specific data formats for the different CSV versions I imported in Snowflake
        - Creating Snowflake **Python User Defined Functions** for some special use cases
        - Using **Astronomer's Cosmos** to integrate dbt tasks into **Airflow**
        - Leveraging Uber's **H3** Hexagonal hierarchical geospatial indexing system for aggregating data
        - Using **Geospatial visualization** tools and libraries, such as **Leafmap, pydeck**
        - Building a **Streamlit** app for interactive visualization
        
        In this project I tried to implement various methods and technologies we learned in the Bootcamp. Through designing and implementing this complex data pipeline I gained valuable experience and improved technical proficiency and problem solving skills.

        #### Next Steps
        There was only a limited time for the project, so there are still a lot of ideas for improvement and for new functionalities.
        - The main data sets are currently only available in parquet files downloadable from a website, but hopefully it will be available through an Amazon S3 bucket in the near future
        - There are plans for more frequent release of the data, which when integrated would offer a more up to date picture
        - I wanted to integrate flight schedule data as well, to analyze flight delays
        - Compare schedule data with weather data from the Open-Meteo API to discover the effects of weather on flight delays
        - Currently the dataset is limited to the start of the year to be mindful of the costs associated but the time scope can be extended easily
    """
    )
st.sidebar.markdown(
    """
    #### Want to learn more about the Bootcamp?

    - Check out [DataExpert.io](https://www.dataexpert.io/)
    - Read the  [DE Handbook](https://github.com/DataExpert-io/data-engineer-handbook)
    - Join the free [DE Bootcamp](https://bootcamp.techcreator.io/)
"""
)
