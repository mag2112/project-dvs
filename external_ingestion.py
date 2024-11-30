import json
import os
from datetime import timedelta
from typing import Any

import yaml
from airflow.hooks.base_hook import BaseHook
from airflow.models.dag import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.providers.http.operators.http import SimpleHttpOperator
from common.config.owners import Owners
from google.auth.transport.requests import Request
from google.oauth2.id_token import fetch_id_token

default_args = {
    "owner": Owners.DATA_ENGINEERING.value,
    "depends_on_past": False,
    "max_active_runs": 1,
}


def load_configurations():
    # Read YAML file
    file_path = os.path.join(
        os.path.dirname(os.path.abspath(__file__)), "external_ingestion.yaml"
    )

    with open(file_path, "r") as f:
        dag_configs = yaml.safe_load(f)
        print("ConfiguraÃ§Ãµes Carregadas do YAML:", dag_configs) 
        return dag_configs


def get_function_params(task_id, endpoint, **task_params):
    print(f"Extraindo parÃ¢metros com task_id: {task_id}, endpoint: {endpoint}, task_params: {task_params}")
    return task_id, endpoint, task_params


def invoke_function(ti, task_params):

    task_id, endpoint, input_params = get_function_params(**task_params)

    print(f"Parametros {input_params}")

    function_conn_id = "http_cloud_function_agronex"
    host = BaseHook.get_connection(function_conn_id).host

    function_url = f"{host}/{endpoint}"
    print(f"URL completa da funÃ§Ã£o: {function_url}")

    TOKEN = fetch_id_token(Request(), function_url)
    print(f"Token obtido: {TOKEN}")

    http = SimpleHttpOperator(
        task_id=task_id,
        execution_timeout=timedelta(hours=1),
        method="POST",
        http_conn_id=function_conn_id,
        endpoint=endpoint,
        headers={
            "Authorization": f"Bearer {TOKEN}",
            "Content-Type": "application/json",
        },
        data=json.dumps(input_params),
    )
    print(f"Executando HTTP Operator com endpoint: {function_url}, headers: {{'Authorization': 'Bearer {TOKEN}', 'Content-Type': 'application/json'}}, data: {json.dumps(input_params)}")
    http.execute(ti)


# Dynamic generate Tasks
def task_generator(dag: DAG, task: Any, task_default_params: dict, option):

    # 20240103: Ajuste para adicionar os parÃ¢metros removidos do yaml ao dic task_default_params
    task_default_params["option"] = option
    # --

    op_kwargs = {"task_params": {**task_default_params, **task}}
    print(f"Gerando tarefa: {task['task_id']} com parÃ¢metros: {op_kwargs}")

    return PythonOperator(
        dag=dag,
        task_id=task["task_id"],
        execution_timeout=timedelta(hours=1),
        op_kwargs=op_kwargs,
        python_callable=invoke_function,
    )


def dag_docs(description: str = "", tasks=[], task_default_params={}, **dag_params):

    # 20240103: Ajuste para listar os parametros removidos do yaml e listar do DAG Docs
    task_default_params_doc = task_default_params
    task_default_params_doc.update({"option": str("{ task_instance.task_id }")})
    
    # --
    
    def list_params(params: dict):
        return "".join(
            (f"\n - **{param}**: `{value}` " for param, value in params.items())
        )

    doc_dag_params = list_params(dag_params)
    doc_task_default_params = list_params(task_default_params_doc)
    doc_tasks = "\n".join(
        (f"\n **{task['task_id']}**\n{list_params(task)}" for task in tasks)
    )

    return f"""
    **{description}**

    **DAG params:**

    {doc_dag_params}

    **Task default params:**

    {doc_task_default_params}

    **Tasks:**

    {doc_tasks}
    """


# Dynamic generate DAGS
def create_dag(dag_id, dag_config: dict, option):
    print(f"Criando DAG: {dag_id} com configuraÃ§Ã£o: {dag_config}")
    with DAG(
        dag_id=dag_id,
        default_args=default_args,
        description=dag_config.get("description", dag_id),
        #schedule_interval=dag_config.get("schedule_interval", "@hourly"),
        schedule_interval="0 6,9,15,20 * * *",
        start_date=dag_config.get("start_date"),
        catchup=dag_config.get("catchup", False),
        max_active_runs=1,
        doc_md=dag_docs(**dag_config),
    ) as dag:

        start = DummyOperator(task_id="start")

        tasks = [
            task_generator(dag, task, dag_config.get("task_default_params", {}), option)
            for task in dag_config.get("tasks", [])
        ]

        end = DummyOperator(task_id="end")

        start >> tasks >> end

        return dag


# Generate DAGS for configurations
configurations = load_configurations()
for id, dag_config in configurations.items():
    dag_id = f"external_ingestion_{id}"
    
    # 20240103: Ajuste para nova versÃ£o do Airflow 2.6.3 passando os parametros removidos do yaml
    option = '{{ task_instance.task_id }}'
    # --
    
    globals()[dag_id] = create_dag(dag_id, dag_config, option)