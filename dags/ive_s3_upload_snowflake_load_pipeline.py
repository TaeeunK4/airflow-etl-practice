from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from airflow.utils.task_group import TaskGroup
from airflow.utils.dates import days_ago
import sys
import os
sys.path.append("/opt/airflow/include/scripts")
from s3_upload_csv import s3_upload_csv

BUCKET_NAME = "ivekorea-airflow-practice-taeeunk"
LOCAL_PATH = "/opt/airflow/data"
SNOWFLAKE_CONN_ID = "snowflake_con"
DATABASE_NAME = "IVE_DATA"
SCHEMA_NAME = "RAW_DATA"
STAGE_NAME = "MY_S3_STAGE"

default_args = {
    "owner" : 'Taeeun',
    "start_date" : days_ago(1),
    "catchup" : False,
}

with DAG(
    dag_id = "ive_s3_upload_snowflake_load_pipeline",
    default_args = default_args,
    schedule_interval = "@daily",
    tags = ["ive", "upload", "to_csv", "s3"]
) as dag:
    # Task 1 : Snowflake WH, DB, SCHEMA, STAGE setup
    with TaskGroup("Snowflake_setup_env") as Snowflake_setup_env:
        setup_env = SnowflakeOperator(
            task_id = "Snowflake_setup_env",
            snowflake_conn_id = SNOWFLAKE_CONN_ID,
            sql=[
                "CREATE WAREHOUSE IF NOT EXISTS COMPUTE_WH WITH WAREHOUSE_SIZE = 'XSMALL' AUTO_SUSPEND = 60 AUTO_RESUME = TRUE;",
                "CREATE DATABASE IF NOT EXISTS {{ var.value.DATABASE_NAME }};",
                "CREATE SCHEMA IF NOT EXISTS {{ var.value.DATABASE_NAME }}.{{ var.value.SCHEMA_NAME }};",
                """
                CREATE OR REPLACE STAGE {{ var.value.DATABASE_NAME }}.{{ var.value.SCHEMA_NAME }}.{{ var.value.STAGE_NAME }}
                URL = 's3://{{ var.value.BUCKET_NAME }}/'
                CREDENTIALS = (
                    AWS_KEY_ID = '{{ var.value.AWS_ACCESS_KEY_ID }}'
                    AWS_SECRET_KEY = '{{ var.value.AWS_SECRET_ACCESS_KEY }}'
                );
                """
    ]
        )
    # Task 2 : ive_list s3 upload -> snowflake load
    with TaskGroup("upload_snowflake_list") as upload_snowflake_list:
        upload_list = PythonOperator(
            task_id = "upload_s3_list",
            python_callable = s3_upload_csv,
            op_kwargs = {
                "local_base_path" : os.path.join(LOCAL_PATH, "ive_list"),
                "file_names" : ["ive_list_all.xlsx"],
                "s3_folder" : "ive_list",
                "bucket_name" : BUCKET_NAME,
                "aws_credentials" : {
                    "AWS_ACCESS_KEY_ID" : "{{var.value.AWS_ACCESS_KEY_ID}}",
                    "AWS_SECRET_ACCESS_KEY" : "{{var.value.AWS_SECRET_ACCESS_KEY}}",
                    "AWS_ACCESS_REGION" : "{{var.value.AWS_ACCESS_REGION}}",
                    },
                "target_columns" : [
                    "ads_idx", "adv_idx", "sch_idx", "ads_type", "ads_category",
                    "ads_name", "ads_summary", "ads_guide", "ads_save_way",
                    "ads_sdate", "ads_edate", "ads_os_type", "ads_contract_price",
                    "ads_reward_price", "ads_order", "ads_rejoin_type", "ads_require_adid"
                    ]
            }
        )
        load_list = SnowflakeOperator(
            task_id = "load_snowflake_list",
            snowflake_conn_id = SNOWFLAKE_CONN_ID,
            sql = [
               """
                CREATE OR REPLACE TABLE {{ var.value.DATABASE_NAME }}.{{ var.value.SCHEMA_NAME }}.IVE_LIST_RAW (
                    ADS_IDX NUMBER, ADV_IDX NUMBER, SCH_IDX NUMBER, ADS_TYPE NUMBER,
                    ADS_CATEGORY NUMBER, ADS_NAME VARCHAR, ADS_SUMMARY VARCHAR,
                    ADS_GUIDE VARCHAR, ADS_SAVE_WAY VARCHAR, ADS_SDATE TIMESTAMP,
                    ADS_EDATE TIMESTAMP, ADS_OS_TYPE NUMBER, ADS_CONTRACT_PRICE NUMBER,
                    ADS_REWARD_PRICE NUMBER, ADS_ORDER NUMBER, ADS_REJOIN_TYPE VARCHAR,
                    ADS_REQUIRE_ADID VARCHAR
                );
                """,
                """
                COPY INTO {{ var.value.DATABASE_NAME }}.{{ var.value.SCHEMA_NAME }}.IVE_LIST_RAW
                FROM @{{ var.value.DATABASE_NAME }}.{{ var.value.SCHEMA_NAME }}.{{ var.value.STAGE_NAME }}/ive_list/
                FILE_FORMAT = (TYPE = 'CSV', SKIP_HEADER = 1, FIELD_OPTIONALLY_ENCLOSED_BY = '"')
                ON_ERROR = 'CONTINUE';
                """ 
            ]
        )
        # upload_snowflake_list task 순서 지정
        upload_list >> load_list

    # Task 3 : ive_sch s3 upload -> snowflake load
    with TaskGroup("upload_snowflake_sch") as upload_snowflake_sch:
        upload_sch = PythonOperator(
            task_id = "upload_s3_sch",
            python_callable = s3_upload_csv,
            op_kwargs = {
                "local_base_path" : os.path.join(LOCAL_PATH, "ive_sch"),
                "file_names" : ["ive_sch_all.xlsx"],
                "s3_folder" : "ive_sch",
                "bucket_name" : BUCKET_NAME,
                "aws_credentials" : {
                    "AWS_ACCESS_KEY_ID" : "{{var.value.AWS_ACCESS_KEY_ID}}",
                    "AWS_SECRET_ACCESS_KEY" : "{{var.value.AWS_SECRET_ACCESS_KEY}}",
                    "AWS_ACCESS_REGION" : "{{var.value.AWS_ACCESS_REGION}}",
                    },
                "target_columns" : [
                    "sch_idx", "ads_idx", "mda_idx_arr",
                    "sch_clk_num", "sch_turn_num", "sch_type"
                    ]
            }
        )
        load_sch = SnowflakeOperator(
            task_id = "load_snowflake_sch",
            snowflake_conn_id = SNOWFLAKE_CONN_ID,
            sql = [
               """
                CREATE OR REPLACE TABLE {{ var.value.DATABASE_NAME }}.{{ var.value.SCHEMA_NAME }}.IVE_SCH_RAW (
                    SCH_IDX NUMBER, ADS_IDX NUMBER, MDA_IDX_ARR VARCHAR,
                    SCH_CLK_NUM NUMBER, SCH_TURN_NUM NUMBER, SCH_TYPE VARCHAR
                );
                """,
                """
                COPY INTO {{ var.value.DATABASE_NAME }}.{{ var.value.SCHEMA_NAME }}.IVE_SCH_RAW
                FROM @{{ var.value.DATABASE_NAME }}.{{ var.value.SCHEMA_NAME }}.{{ var.value.STAGE_NAME }}/ive_sch/
                FILE_FORMAT = (TYPE = 'CSV', SKIP_HEADER = 1, FIELD_OPTIONALLY_ENCLOSED_BY = '"')
                ON_ERROR = 'CONTINUE';
                """ 
            ]
        )
        # upload_snowflake_sch task 순서 지정
        upload_sch >> load_sch

    # Task 4 : ive_year s3 upload -> snowflake load
    with TaskGroup("upload_snowflake_year") as upload_snowflake_year:
        YEAR_PATH = os.path.join(LOCAL_PATH, "ive_year")
        # ive_year_{i} all check
        if os.path.exists(YEAR_PATH):
            year_all_files = os.listdir(YEAR_PATH)
            year_files = [i for i in year_all_files if i.startswith("ive_year") and i.endswith(".csv")]
            year_files.sort()
        else:
            year_files = []   

        upload_year = PythonOperator(
            task_id = "upload_s3_year",
            python_callable = s3_upload_csv,
            op_kwargs = {
                "local_base_path" : YEAR_PATH,
                "file_names" : year_files,
                "s3_folder" : "ive_year",
                "bucket_name" : BUCKET_NAME,
                "aws_credentials" : {
                    "AWS_ACCESS_KEY_ID" : "{{var.value.AWS_ACCESS_KEY_ID}}",
                    "AWS_SECRET_ACCESS_KEY" : "{{var.value.AWS_SECRET_ACCESS_KEY}}",
                    "AWS_ACCESS_REGION" : "{{var.value.AWS_ACCESS_REGION}}",
                    },
                "target_columns" : [
                    "rpt_time_date", "rpt_time_time", "ads_idx", "mda_idx",
                    "rpt_time_clk", "rpt_time_turn", "rpt_time_scost", "rpt_time_acost",
                    "rpt_time_cost", "rpt_time_earn"
                    ]
            }
        )
        load_year = SnowflakeOperator(
            task_id = "load_snowflake_year",
            snowflake_conn_id = SNOWFLAKE_CONN_ID,
            sql = [
               """
                CREATE OR REPLACE TABLE {{ var.value.DATABASE_NAME }}.{{ var.value.SCHEMA_NAME }}.IVE_YEAR_RAW (
                    RPT_TIME_DATE DATE, RPT_TIME_TIME TIMESTAMP, ADS_IDX NUMBER, MDA_IDX NUMBER,
                    RPT_TIME_CLK NUMBER, RPT_TIME_TURN NUMBER, RPT_TIME_SCOST NUMBER , RPT_TIME_ACOST NUMBER,
                    RPT_TIME_COST NUMBER, RPT_TIME_EARN NUMBER
                );
                """,
                """
                COPY INTO {{ var.value.DATABASE_NAME }}.{{ var.value.SCHEMA_NAME }}.IVE_YEAR_RAW
                FROM @{{ var.value.DATABASE_NAME }}.{{ var.value.SCHEMA_NAME }}.{{ var.value.STAGE_NAME }}/ive_year/
                FILE_FORMAT = (TYPE = 'CSV', SKIP_HEADER = 1, FIELD_OPTIONALLY_ENCLOSED_BY = '"')
                ON_ERROR = 'CONTINUE';
                """ 
            ]
        )
        # upload_snowflake_year task 순서 지정
        upload_year >> load_year 

Snowflake_setup_env >> [upload_snowflake_list, upload_snowflake_sch, upload_snowflake_year]