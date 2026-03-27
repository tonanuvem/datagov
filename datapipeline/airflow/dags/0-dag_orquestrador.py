"""
DAG Orquestradora do Pipeline de Dados
=======================================
Orquestra execução sequencial: Bronze → Silver → Gold → Catalog

CORREÇÃO APLICADA (deadlock de slots no LocalExecutor):
  Problema: TriggerDagRunOperator com wait_for_completion=True ocupa um slot
  do executor enquanto aguarda a DAG filha. Com LocalExecutor, a DAG filha
  não consegue pegar slot para executar → deadlock silencioso.

  Solução 1 (principal): deferrable=True
    Libera o slot do worker enquanto aguarda, usando o Triggerer do Airflow 3.x.
    Requer que o serviço `triggerer` esteja rodando (já incluso na imagem
    docker.getcollate.io/openmetadata/ingestion:1.12.0).

  Solução 2 (fallback): pool dedicado com slots suficientes.
    Se o triggerer não estiver disponível, um pool 'pipeline_pool' com
    slots >= número de DAGs em execução simultânea evita o deadlock.

  Ambas as soluções estão aplicadas aqui.

COMO CRIAR O POOL (execute uma vez no Airflow UI ou CLI):
  airflow pools set pipeline_pool 8 "Pool para DAGs do pipeline de alunos"
"""

from airflow import DAG
from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator
from datetime import datetime, timedelta

default_args = {
    "owner":            "engenharia.dados@empresa.com",
    "depends_on_past":  False,
    "email_on_failure": False,
    "email_on_retry":   False,
    "retries":          0,
    # Timeout generoso — cada DAG filha pode levar alguns minutos
    "execution_timeout": timedelta(hours=2),
}

# ─── Helper para criar operators padronizados ─────────────────────────────────

def make_trigger(task_id: str, trigger_dag_id: str) -> TriggerDagRunOperator:
    """
    Cria um TriggerDagRunOperator com:
      - deferrable=True  → libera slot enquanto aguarda (Airflow 3.x Triggerer)
      - pool             → fallback para evitar deadlock se triggerer indisponível
      - wait_for_completion=True → orquestrador só avança após DAG filha terminar
      - failed_states    → qualquer falha na filha propaga erro para o orquestrador
    """
    return TriggerDagRunOperator(
        task_id=task_id,
        trigger_dag_id=trigger_dag_id,
        wait_for_completion=True,
        deferrable=True,            # ← correção principal: libera slot do worker
        poke_interval=15,           # polling a cada 15s (só usado se deferrable=False)
        reset_dag_run=True,
        allowed_states=["success"],
        failed_states=["failed", "upstream_failed"],
        pool="pipeline_pool",       # ← fallback: pool dedicado com slots suficientes
    )


# ─── DAG ─────────────────────────────────────────────────────────────────────

with DAG(
    dag_id="0_dag_orquestrador",
    default_args=default_args,
    description=(
        "Orquestra execução sequencial do pipeline: "
        "Bronze → Silver → Gold → Catalog (catalogação em paralelo)"
    ),
    schedule="0 6 * * *",   # Diariamente às 6h
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["orquestrador", "pipeline"],
    # Garante que apenas 1 execução do orquestrador rode por vez
    max_active_runs=1,
) as dag:

    # ── Bronze ────────────────────────────────────────────────────────────────
    trigger_bronze = make_trigger(
        task_id="trigger_bronze_ingestion",
        trigger_dag_id="1_bronze_ingestion",
    )
    trigger_bronze_test = make_trigger(
        task_id="trigger_bronze_test",
        trigger_dag_id="2_bronze_test",
    )

    # ── Silver ────────────────────────────────────────────────────────────────
    trigger_silver = make_trigger(
        task_id="trigger_silver_transformation",
        trigger_dag_id="3_silver_transformation",
    )
    trigger_silver_test = make_trigger(
        task_id="trigger_silver_test",
        trigger_dag_id="4_silver_test",
    )

    # ── Gold ──────────────────────────────────────────────────────────────────
    trigger_gold = make_trigger(
        task_id="trigger_gold_aggregation",
        trigger_dag_id="5_gold_aggregation",
    )
    trigger_gold_test = make_trigger(
        task_id="trigger_gold_test",
        trigger_dag_id="6_gold_test",
    )

    # ── Catalog (paralelo após gold_test) ─────────────────────────────────────
    trigger_catalog_csv_local = make_trigger(
        task_id="trigger_catalog_csv_local",
        trigger_dag_id="7_catalog_csv_local",
    )
    trigger_catalog_openmetadata = make_trigger(
        task_id="trigger_catalog_openmetadata",
        trigger_dag_id="7_catalog_openmetadata",
    )

    # ── Sequência ─────────────────────────────────────────────────────────────
    (
        trigger_bronze
        >> trigger_bronze_test
        >> trigger_silver
        >> trigger_silver_test
        >> trigger_gold
        >> trigger_gold_test
        >> [trigger_catalog_csv_local, trigger_catalog_openmetadata]
    )
