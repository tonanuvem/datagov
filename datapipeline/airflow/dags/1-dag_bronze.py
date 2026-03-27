"""
DAG BRONZE - Ingestão de Dados Crus

INPUTS:  /opt/nb/curso.txt          (fonte CSV)
OUTPUT:  /opt/nb/bronze/alunos_raw.csv

Linhagem no OpenMetadata:
  curso_txt (Table) ──► [1_bronze_ingestion DAG] ──► alunos_raw (Table)

COMO FUNCIONA:
  - inlets  → declaram a FONTE que esta task consome
  - outlets → declaram o DESTINO que esta task produz
  - O Lineage Backend do OM captura esses metadados ao fim de cada run
    e cria arestas no grafo: Pipeline → Tabela catalogada no OM.

PRÉ-REQUISITO:
  Execute a DAG 7_catalog_openmetadata ANTES, pois ela registra as tabelas
  no OpenMetadata. O Lineage Backend precisa encontrar as entidades pelo FQN.
"""

from __future__ import annotations

import os
from datetime import datetime, timedelta

from airflow.sdk import dag, task

# Airflow 3.x: Asset substitui Dataset para declarar dependências entre DAGs
# e é o tipo correto para inlets/outlets reconhecidos pelo OM Lineage Backend.
from airflow.sdk import Asset

import pandas as pd

# ── URIs das entidades no OpenMetadata (usados como inlets/outlets) ────────────
# O Lineage Backend mapeia o URI do Asset para o FQN da tabela no OM.
# Formato: openmetadata://<service>.<database>.<schema>.<table>

ASSET_FONTE   = Asset(uri="openmetadata://pipeline_alunos.educacao.camadas.curso_txt")
ASSET_BRONZE  = Asset(uri="openmetadata://pipeline_alunos.educacao.camadas.alunos_raw")


@dag(
    dag_id="1_bronze_ingestion",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["bronze", "ingestion"],
    description="Ingestão de dados brutos na camada Bronze",
    owner_links={"owner": "mailto:engenharia.dados@empresa.com"},
)
def bronze_pipeline():

    @task(
        task_id="ingest_raw_data",
        execution_timeout=timedelta(minutes=2, seconds=30),
        # inlets  → o que esta task CONSOME  (fonte CSV)
        # outlets → o que esta task PRODUZ   (bronze CSV → tabela OM)
        inlets=[ASSET_FONTE],
        outlets=[ASSET_BRONZE],
    )
    def ingest_raw_data():
        """Ingere dados brutos do arquivo curso.txt para a camada Bronze."""
        print("=== INICIANDO INGESTÃO BRONZE ===")

        input_path  = "/opt/nb/curso.txt"
        output_path = "/opt/nb/bronze/alunos_raw.csv"

        print(f"Lendo arquivo: {input_path}")
        df = pd.read_csv(input_path)
        print(f"✅ Arquivo lido: {len(df)} registros")

        df["data_ingestao"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        df["fonte"]         = "curso.txt"
        print("✅ Metadados adicionados")

        os.makedirs(os.path.dirname(output_path), exist_ok=True)
        df.to_csv(output_path, index=False)
        print(f"✅ Arquivo salvo: {output_path}")

        msg = f"✅ BRONZE CONCLUÍDO: {len(df)} registros salvos"
        print(msg)
        return msg

    ingest_raw_data()


bronze_pipeline()
