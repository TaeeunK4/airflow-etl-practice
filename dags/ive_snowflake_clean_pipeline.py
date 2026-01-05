from airflow import DAG
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from airflow.utils.task_group import TaskGroup
from airflow.utils.dates import days_ago
import os

SNOWFLAKE_CONN_ID = "snowflake_con"

default_args = {
    "owner" : 'Taeeun',
    "start_date" : days_ago(1),
    "catchup" : False,
}

with DAG(
    dag_id = "ive_snowflake_clean_pipeline",
    default_args = default_args,
    schedule_interval = "@daily",
    template_searchpath = [
        '/opt/airflow/dbt_project/models/clean',
        '/opt/airflow/dbt_project/models/left_join',        
        '/opt/airflow/dbt_project/models/utils'        
    ],
    tags = ["ive", "clean"]
) as dag:
    with TaskGroup("Snowflake_clean_list") as Snowflake_clean_list:
        clean_list = SnowflakeOperator(
            task_id = "Snowflake_clean_list",
            snowflake_conn_id = SNOWFLAKE_CONN_ID,
            sql = "clean_list.sql"
        )
    
    with TaskGroup("Snowflake_clean_sch") as Snowflake_clean_sch:
        clean_sch = SnowflakeOperator(
            task_id = "Snowflake_clean_sch",
            snowflake_conn_id = SNOWFLAKE_CONN_ID,
            sql = "clean_sch.sql"
        )

    with TaskGroup("Snowflake_clean_year") as Snowflake_clean_year:
        clean_year = SnowflakeOperator(
            task_id = "Snowflake_clean_year",
            snowflake_conn_id = SNOWFLAKE_CONN_ID,
            sql = "clean_year.sql"
        )
    
    with TaskGroup("Snowflake_year_list_sch_join") as Snowflake_year_list_sch_join:
        year_list_sch_join = SnowflakeOperator(
            task_id = "Snowflake_year_list_sch_join",
            snowflake_conn_id = SNOWFLAKE_CONN_ID,
            sql = "left_join.sql"
        )

    with TaskGroup("Snowflake_s3_upload") as Snowflake_s3_upload:
        snowflake_s3_upload = SnowflakeOperator(
            task_id = "Snowflake_s3_upload",
            snowflake_conn_id = SNOWFLAKE_CONN_ID,
            sql = "s3_upload.sql"
        )

[Snowflake_clean_list, Snowflake_clean_sch, Snowflake_clean_year] >> Snowflake_year_list_sch_join >> Snowflake_s3_upload