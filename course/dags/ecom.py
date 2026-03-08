from airflow.decorators import dag
from pendulum import datetime,duration
from airflow.operators.empty import EmptyOperator
from airflow.datasets import Dataset

DATASET_COCKTAIL=Dataset('/tmp/cocktail.json')

@dag(
    start_date=datetime(2025,1,1),
    schedule=[DATASET_COCKTAIL],
    catchup=False,
    description="This dag processes ecommerce data",
    tags=["team_a","ecom"],
    default_args={"retries":1},
    dagrun_timeout=duration(minutes=20),
    max_consecutive_failed_dag_runs=2
)

def ecom():

    ta=EmptyOperator(
        task_id='ta'
    )

ecom()