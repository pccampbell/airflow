from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import timedelta

from nfl_dl.loaders.pbp_loader import load_current_season
from nfl_dl.utils.io import df_to_stringio

TABLE_NAME = "nfl_pbp"
POSTGRES_CONN_ID = "nfl_postgres"

def refresh_current_season():
    df, season = load_current_season()
    pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    conn = pg_hook.get_conn()
    cur = conn.cursor()
    try:
        cur.execute(f"DELETE FROM {TABLE_NAME} WHERE season = %s;", (season,))
        buf = df_to_stringio(df)
        cur.copy_expert(f"COPY {TABLE_NAME} FROM STDIN WITH CSV", buf)
        conn.commit()
        print(f"✅ Reloaded {len(df)} rows for {season}")
    except:
        conn.rollback()
        raise
    finally:
        cur.close()
        conn.close()

with DAG(
    dag_id="nfl_pbp_refresh",
    default_args={"owner": "airflow", "retries": 1, "retry_delay": timedelta(minutes=10)},
    description="Daily refresh of current season PBP",
    schedule_interval="0 3 * * *",
    start_date=days_ago(1),
    catchup=False,
    max_active_runs=1,
) as dag:
    PythonOperator(
        task_id="refresh_current_season",
        python_callable=refresh_current_season,
    )