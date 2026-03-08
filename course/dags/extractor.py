from airflow.decorators import dag,task
from airflow.operators.python import PythonOperator
from pendulum import datetime
from airflow.datasets import Dataset
from airflow.exceptions import AirflowFailException
import json
from airflow.utils.trigger_rule import TriggerRule

DATASET_COCKTAIL=Dataset('/tmp/cocktail.json')

def _get_cocktail(ti=None):
    import requests

    api="https://www.thecocktaildb.com/api/json/v1/1/random.php"
    response=requests.get(api)
    with open(DATASET_COCKTAIL.uri,"wb") as f:
        f.write(response.content)
    ti.xcom_push(key='request_size',value=len(response.content))

def _check_size(ti=None):
    size=ti.xcom_pull(key='request_size',task_ids='get_cocktail')
    # print(size)
    # raise AirflowFailException
    if(size <=0):
        raise AirflowFailException

def handle_failed_dag_run(context):
    print(f"""DAG run failed with the task {context['task_instance'].task_id} 
    for the data interval between {context['prev_ds']} and {context['next_ds']}""")

@dag(
    start_date=datetime(2025,1,1),
    schedule='@daily',
    catchup=False,
    on_failure_callback=handle_failed_dag_run
)

def extractor():

    get_cocktail=PythonOperator(
        task_id='get_cocktail',
        python_callable=_get_cocktail,
        outlets=[DATASET_COCKTAIL]
    )

    check_size=PythonOperator(
        task_id='check_size',
        python_callable=_check_size
    )

    @task.branch()
    def branch_cocktail_type():
        with open(DATASET_COCKTAIL.uri,'r') as f:
            data=json.load(f)
        if(data['drinks'][0]['strAlcoholic']=='Alcoholic'):
            return 'alcoholic_drink'
        else:
            return 'non_alcoholic_drink'

    @task
    def alcoholic_drink():
        print('alcoholic')

    @task
    def non_alcoholic_drink():
        print('non-alcoholic')

    @task(trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS, templates_dict={'the_current_date':'{{ ds }}'})
    def execute(templates_dict):
        print("executed")
        print(f"for the date {templates_dict['the_current_date']}")

    get_cocktail >> check_size >> branch_cocktail_type() >> [alcoholic_drink(),non_alcoholic_drink()] >> execute()

extractor()