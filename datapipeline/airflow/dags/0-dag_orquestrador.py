"""
DAG Orquestradora do Pipeline de Dados
Esta DAG orquestra a execução sequencial de todas as etapas do pipeline:
Bronze → Silver → Gold → Catalog, incluindo testes em cada camada.

Se qualquer etapa falhar, o pipeline é interrompido.
"""
from airflow import DAG
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'engenharia.dados@empresa.com',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
}

with DAG(
    dag_id='pipeline_orquestrador',
    default_args=default_args,
    description='Orquestra execução sequencial do pipeline Bronze → Silver → Gold → Catalog',
    schedule_interval='0 6 * * *',  # Diariamente às 6h
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['orquestrador', 'pipeline'],
) as dag:

    # Bronze: Ingestão
    trigger_bronze = TriggerDagRunOperator(
        task_id='trigger_bronze_ingestion',
        trigger_dag_id='bronze_ingestion',
        wait_for_completion=True,
        poke_interval=30,
        reset_dag_run=True,
    )

    # Bronze: Teste
    trigger_bronze_test = TriggerDagRunOperator(
        task_id='trigger_bronze_test',
        trigger_dag_id='bronze_test',
        wait_for_completion=True,
        poke_interval=30,
        reset_dag_run=True,
    )

    # Silver: Transformação
    trigger_silver = TriggerDagRunOperator(
        task_id='trigger_silver_transformation',
        trigger_dag_id='silver_transformation',
        wait_for_completion=True,
        poke_interval=30,
        reset_dag_run=True,
    )

    # Silver: Teste
    trigger_silver_test = TriggerDagRunOperator(
        task_id='trigger_silver_test',
        trigger_dag_id='silver_test',
        wait_for_completion=True,
        poke_interval=30,
        reset_dag_run=True,
    )

    # Gold: Agregação
    trigger_gold = TriggerDagRunOperator(
        task_id='trigger_gold_aggregation',
        trigger_dag_id='gold_aggregation',
        wait_for_completion=True,
        poke_interval=30,
        reset_dag_run=True,
    )

    # Gold: Teste
    trigger_gold_test = TriggerDagRunOperator(
        task_id='trigger_gold_test',
        trigger_dag_id='gold_test',
        wait_for_completion=True,
        poke_interval=30,
        reset_dag_run=True,
    )

    # Catalog: Catalogação
    trigger_catalog = TriggerDagRunOperator(
        task_id='trigger_catalog_metadata',
        trigger_dag_id='catalog_metadata',
        wait_for_completion=True,
        poke_interval=30,
        reset_dag_run=True,
    )

    # Definir sequência de execução
    trigger_bronze >> trigger_bronze_test >> trigger_silver >> trigger_silver_test >> trigger_gold >> trigger_gold_test >> trigger_catalog
