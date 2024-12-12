import os
import requests
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
from airflow.decorators import dag
from airflow.utils.dates import datetime
from airflow.operators.python_operator import PythonOperator
from airflow.models import Variable
from snowflake.snowpark import Session
import snowflake.connector
from airflow.sensors.external_task_sensor import ExternalTaskSensor
from airflow.utils.state import State
from airflow.utils.task_group import TaskGroup
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator
from cosmos import DbtTaskGroup, ProjectConfig, ProfileConfig, RenderConfig
from dotenv import load_dotenv
from include.eczachly.trino_queries import run_trino_query_dq_check, execute_trino_query
from airflow.configuration import conf


s3_bucket = Variable.get("AWS_S3_BUCKET_TABULAR")
tabular_credential = Variable.get("TABULAR_CREDENTIAL")
catalog_name = Variable.get("CATALOG_NAME")
aws_region = Variable.get("AWS_GLUE_REGION")
aws_access_key_id = Variable.get("DATAEXPERT_AWS_ACCESS_KEY_ID")
aws_secret_access_key = Variable.get("DATAEXPERT_AWS_SECRET_ACCESS_KEY")


def get_snowpark_session(schema="dbcake"):
    connection_params["schema"] = schema
    session = Session.builder.configs(connection_params).create()
    return session


def run_snowflake_query_dq_check(query):
    results = execute_snowflake_query(query)
    if len(results) == 0:
        raise ValueError("The query returned no results!")
    for result in results:
        for column in result:
            if type(column) is bool:
                assert column is True


def execute_snowflake_query(query):
    # Establish a connection to Snowflake
    conn = snowflake.connector.connect(**connection_params)
    try:
        # Create a cursor object to execute queries
        cursor = conn.cursor()
        # Example query: Get the current date from Snowflake
        cursor.execute(query)
        # Fetch and print the result
        result = cursor.fetchall()
        return result
    finally:
        # Close the cursor and connection
        cursor.close()
        conn.close()


def download_file(url, local_path):
    with requests.get(url, stream=True) as r:
        r.raise_for_status()
        with open(local_path, "wb") as f:
            for chunk in r.iter_content(chunk_size=8192):
                f.write(chunk)
    return local_path


def upload_file_to_sf_stage(local_file, stage):
    session = get_snowpark_session(schema="dbcake")
    put_result = session.file.put(local_file_name=local_file, stage_location=stage, overwrite=True)
    print(put_result[0].status)


def clean_up(local_path):
    os.remove(local_path)


def get_file_url():
    URI_ROOT = "https://www.eurocontrol.int/performance/data/download/OPDI/v002"
    DIR = "flight_list"
    FILE = "flight_list"
    EXTENSION = "parquet"
    date_part = "{{ ds_nodash[:6] }}"
    file_name = f"{URI_ROOT}/{DIR}/{FILE}_{date_part}.{EXTENSION}"
    return file_name


dbt_env_path = os.path.join(os.environ["AIRFLOW_HOME"], "dbt_project", "dbt.env")
load_dotenv(dbt_env_path)

# Retrieve environment variables
airflow_home = os.getenv("AIRFLOW_HOME")
PATH_TO_DBT_PROJECT = f"{airflow_home}/dbt_project"
PATH_TO_DBT_PROFILES = f"{airflow_home}/dbt_project/profiles.yml"

profile_config = ProfileConfig(
    profile_name="jaffle_shop",
    target_name="dev",
    profiles_yml_filepath=PATH_TO_DBT_PROFILES,
)


@dag(
    description="DAG to upload file to Snowflake stage",
    default_args={
        "owner": "dbcake",
        "start_date": datetime(2024, 1, 1),
        "end_date": datetime(2024, 2, 1),
        "retries": 0,
        "depends_on_past": True,
    },
    max_active_runs=1,
    schedule_interval="@monthly",
    catchup=True,
    tags=["project", "dbcake", "capstone", "Snowflake"],
    template_searchpath="include/eczachly",
)
def dbcake_cap_monthly():
    sf_stage = "dbcake_project"
    database = "dataexpert_student"
    schema = "dbcake"
    # start = DummyOperator(task_id="start", wait_for_downstream=True)
    start = ExternalTaskSensor(
        task_id="previous_dagrun_success_check",
        external_dag_id="dbcake_cap_monthly",
        external_task_id=None,
        execution_delta=relativedelta(months=1),
        check_existence=True,
        allowed_states=[State.SUCCESS],
    )
    with TaskGroup("flights") as flights:
        file_format = "parquet_format"
        table_schema = "id STRING, adep STRING, ades STRING, icao24 STRING, FLT_ID STRING, first_seen STRING, last_seen STRING, DOF STRING, version STRING"
        url = get_file_url()
        filename = url.split("/")[-1]
        local_path = "/tmp/" + filename
        default_output_table = "src_flights_parquet"

        download_source_file = PythonOperator(
            task_id="download_file_from_web",
            depends_on_past=True,
            python_callable=download_file,
            op_kwargs={
                "url": url,
                "local_path": local_path,
            },
        )

        upload_file_to_stage = PythonOperator(
            task_id="upload_file_to_stage",
            depends_on_past=True,
            python_callable=upload_file_to_sf_stage,
            op_kwargs={
                "local_file": local_path,
                "stage": sf_stage,
            },
        )

        create_sf_table = PythonOperator(
            task_id="create_sf_table",
            python_callable=execute_snowflake_query,
            op_kwargs={
                "query": f"""
                    CREATE OR REPLACE TRANSIENT TABLE {schema}.{ default_output_table } ( {table_schema  }) DATA_RETENTION_TIME_IN_DAYS = 0;
                """
            },
        )

        copy_into_sf_table = PythonOperator(
            task_id="copy_into_sf_table",
            python_callable=execute_snowflake_query,
            op_kwargs={
                "query": f"""
                    COPY INTO  {database}.{schema}.{ default_output_table }
                    FROM @{ sf_stage}/{filename}
                    FILE_FORMAT = {file_format }
                    MATCH_BY_COLUMN_NAME=CASE_INSENSITIVE;
                """
            },
        )

        clean_up_temp = PythonOperator(
            task_id="clean_up_temp",
            depends_on_past=True,
            python_callable=clean_up,
            op_kwargs={
                "local_path": local_path,
            },
        )

        run_data_quality_check = PythonOperator(
            task_id="run_data_quality_check",
            python_callable=run_snowflake_query_dq_check,
            op_kwargs={
                "query": f"""
                    SELECT
                        COUNT(CASE WHEN id IS NULL THEN 1 END) = 0 as id_is_not_null_check,
                        COUNT(CASE WHEN icao24 IS NULL THEN 1 END) = 0 as icao24_is_not_null_check,
                        COUNT(CASE WHEN first_seen IS NULL THEN 1 END) = 0 as first_seen_is_not_null_check,
                        COUNT(CASE WHEN last_seen IS NULL THEN 1 END) = 0 as last_seen_is_not_null_check,
                        COUNT(CASE WHEN DOF IS NULL THEN 1 END) = 0 as DOF_is_not_null_check,
                        COUNT(CASE WHEN version IS NULL THEN 1 END) = 0 as version_is_not_null_check,
                        COUNT(1) > 0 AS is_there_data_check
                    FROM {schema}.{default_output_table}
                """
            },
        )

        delete_from_stage = PythonOperator(
            task_id="delete_from_stage",
            python_callable=execute_snowflake_query,
            op_kwargs={
                "query": f"""
                    REMOVE @{sf_stage}/{filename}
                """
            },
        )

        (
            download_source_file
            >> upload_file_to_stage
            >> create_sf_table
            >> copy_into_sf_table
            >> clean_up_temp
            >> run_data_quality_check
            >> delete_from_stage
        )
    with TaskGroup("aircrafts") as aircrafts:

        url = "https://s3.opensky-network.org/data-samples/metadata/aircraft-database-complete-2024-10.csv"
        filename = url.split("/")[-1]
        local_path = "/tmp/" + filename
        default_output_table = "src_aircrafts_csv"
        file_format = "aircrafts_csv_format"
        table_schema = "icao24 STRING, timestamp STRING, acars STRING, adsb STRING, built STRING, categoryDescription STRING, country STRING, engines STRING, firstFlightDate STRING, firstSeen STRING, icaoAircraftClass STRING, lineNumber STRING, manufacturerIcao STRING, manufacturerName STRING, model STRING, modes STRING, nextReg STRING, operator STRING, operatorCallsign STRING, operatorIata STRING, operatorIcao STRING, owner STRING, prevReg STRING, regUntil STRING, registered STRING, registration STRING, selCal STRING, serialNumber STRING, status STRING, typecode STRING, vdl STRING"

        download_source_file = PythonOperator(
            task_id="download_file_from_web",
            depends_on_past=True,
            python_callable=download_file,
            op_kwargs={
                "url": url,
                "local_path": local_path,
            },
        )

        upload_file_to_stage = PythonOperator(
            task_id="upload_file_to_stage",
            depends_on_past=True,
            python_callable=upload_file_to_sf_stage,
            op_kwargs={
                "local_file": local_path,
                "stage": sf_stage,
            },
        )

        create_sf_table = PythonOperator(
            task_id="create_sf_table",
            python_callable=execute_snowflake_query,
            op_kwargs={
                "query": f"""
                    CREATE OR REPLACE TRANSIENT TABLE {schema}.{ default_output_table } ( {table_schema  }) DATA_RETENTION_TIME_IN_DAYS = 0;
                """
            },
        )

        copy_into_sf_table = PythonOperator(
            task_id="copy_into_sf_table",
            python_callable=execute_snowflake_query,
            op_kwargs={
                "query": f"""
                    COPY INTO  {database}.{schema}.{ default_output_table }
                    FROM @{ sf_stage}/{filename}
                    FILE_FORMAT = {file_format }
                    MATCH_BY_COLUMN_NAME=CASE_INSENSITIVE;
                """
            },
        )

        clean_up_temp = PythonOperator(
            task_id="clean_up_temp",
            depends_on_past=True,
            python_callable=clean_up,
            op_kwargs={
                "local_path": local_path,
            },
        )

        run_data_quality_check = PythonOperator(
            task_id="run_data_quality_check",
            python_callable=run_snowflake_query_dq_check,
            op_kwargs={
                "query": f"""
                    SELECT
                        COUNT(CASE WHEN icao24 IS NULL THEN 1 END) = 0 as icao24_is_not_null_check,
                        COUNT(1) > 0 AS is_there_data_check
                    FROM {schema}.{default_output_table}
                """
            },
        )

        delete_from_stage = PythonOperator(
            task_id="delete_from_stage",
            python_callable=execute_snowflake_query,
            op_kwargs={
                "query": f"""
                    REMOVE @{sf_stage}/{filename}
                """
            },
        )

        (
            download_source_file
            >> upload_file_to_stage
            >> create_sf_table
            >> copy_into_sf_table
            >> clean_up_temp
            >> run_data_quality_check
            >> delete_from_stage
        )
    with TaskGroup("aircraft_types") as aircraft_types:

        url = "https://s3.opensky-network.org/data-samples/metadata/doc8643AircraftTypes.csv"
        filename = url.split("/")[-1]
        local_path = "/tmp/" + filename
        default_output_table = "src_aircraft_types_csv"
        file_format = "aircraft_types_csv_format"
        table_schema = "AircraftDescription STRING, Description STRING, Designator STRING, EngineCount STRING, EngineType  STRING, ManufacturerCode STRING, ModelFullName STRING, WTC STRING"

        download_source_file = PythonOperator(
            task_id="download_file_from_web",
            depends_on_past=True,
            python_callable=download_file,
            op_kwargs={
                "url": url,
                "local_path": local_path,
            },
        )

        upload_file_to_stage = PythonOperator(
            task_id="upload_file_to_stage",
            depends_on_past=True,
            python_callable=upload_file_to_sf_stage,
            op_kwargs={
                "local_file": local_path,
                "stage": sf_stage,
            },
        )

        create_sf_table = PythonOperator(
            task_id="create_sf_table",
            python_callable=execute_snowflake_query,
            op_kwargs={
                "query": f"""
                    CREATE OR REPLACE TRANSIENT TABLE {schema}.{ default_output_table } ( {table_schema  }) DATA_RETENTION_TIME_IN_DAYS = 0;
                """
            },
        )

        copy_into_sf_table = PythonOperator(
            task_id="copy_into_sf_table",
            python_callable=execute_snowflake_query,
            op_kwargs={
                "query": f"""
                    COPY INTO  {database}.{schema}.{ default_output_table }
                    FROM @{ sf_stage}/{filename}
                    FILE_FORMAT = {file_format }
                    MATCH_BY_COLUMN_NAME=CASE_INSENSITIVE;
                """
            },
        )

        clean_up_temp = PythonOperator(
            task_id="clean_up_temp",
            depends_on_past=True,
            python_callable=clean_up,
            op_kwargs={
                "local_path": local_path,
            },
        )

        run_data_quality_check = PythonOperator(
            task_id="run_data_quality_check",
            python_callable=run_snowflake_query_dq_check,
            op_kwargs={
                "query": f"""
                    SELECT
                        COUNT(CASE WHEN AircraftDescription IS NULL THEN 1 END) = 0 as AircraftDescription_is_not_null_check,
                        COUNT(CASE WHEN Description IS NULL THEN 1 END) = 0 as Description_is_not_null_check,
                        COUNT(CASE WHEN Designator IS NULL THEN 1 END) = 0 as Designator_is_not_null_check,
                        COUNT(CASE WHEN EngineCount IS NULL THEN 1 END) = 0 as EngineCount_is_not_null_check,
                        COUNT(CASE WHEN EngineType IS NULL THEN 1 END) = 0 as EngineType_is_not_null_check, 
                        COUNT(CASE WHEN ManufacturerCode IS NULL THEN 1 END) = 0 as ManufacturerCode_is_not_null_check,
                        COUNT(CASE WHEN ModelFullName IS NULL THEN 1 END) = 0 as ModelFullName_is_not_null_check,
                        COUNT(CASE WHEN WTC IS NULL THEN 1 END) = 0 as WTC_is_not_null_check,
                        COUNT(1) > 0 AS is_there_data_check
                    FROM {schema}.{default_output_table}
                """
            },
        )
        delete_from_stage = PythonOperator(
            task_id="delete_from_stage",
            python_callable=execute_snowflake_query,
            op_kwargs={
                "query": f"""
                    REMOVE @{sf_stage}/{filename}
                """
            },
        )
        (
            download_source_file
            >> upload_file_to_stage
            >> create_sf_table
            >> copy_into_sf_table
            >> clean_up_temp
            >> run_data_quality_check
            >> delete_from_stage
        )
    dbt_build_fct_flights = DbtTaskGroup(
        group_id="dbt_build_fct_flights",
        project_config=ProjectConfig(PATH_TO_DBT_PROJECT),
        profile_config=profile_config,
        render_config=RenderConfig(
            select=["+fct_flights"],
        ),
    )
    dbt_build_dim_aircrafts = DbtTaskGroup(
        group_id="dbt_build_dim_aircrafts",
        project_config=ProjectConfig(PATH_TO_DBT_PROJECT),
        profile_config=profile_config,
        render_config=RenderConfig(
            select=["+dim_aircrafts"],
        ),
    )

    clear_staging_tables = PythonOperator(
        task_id="clear_staging_tables",
        python_callable=execute_snowflake_query,
        op_kwargs={
            "query": f"""
                    CALL drop_staging_flights_aircrafts();
                """
        },
    )

    end = DummyOperator(task_id="end")

    start >> [flights, aircrafts, aircraft_types]
    [aircrafts, aircraft_types] >> dbt_build_dim_aircrafts
    flights >> dbt_build_fct_flights
    [dbt_build_fct_flights, dbt_build_dim_aircrafts] >> clear_staging_tables >> end


dbcake_cap_monthly()
