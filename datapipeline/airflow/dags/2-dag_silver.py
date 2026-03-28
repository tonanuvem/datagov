"""
DAG SILVER - Transformação de Dados

INPUTS:  /opt/nb/bronze/alunos_raw.csv
OUTPUT:  /opt/nb/silver/alunos_transformado.csv

Linhagem no OpenMetadata:
  alunos_raw (Table) ──► [3_silver_transform DAG] ──► alunos_transformado (Table)
"""

from __future__ import annotations

import os
from datetime import datetime, timedelta

from airflow.sdk import dag, task, Asset
import pandas as pd

ASSET_BRONZE = Asset(uri="openmetadata://pipeline_alunos.educacao.camadas.alunos_raw")
ASSET_SILVER = Asset(uri="openmetadata://pipeline_alunos.educacao.camadas.alunos_transformado")


@dag(
    dag_id="3_silver_transform",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["silver", "transform"],
    description="Transformação e enriquecimento de dados na camada Silver",
    owner_links={"owner": "mailto:engenharia.dados@empresa.com"},
)
def silver_pipeline():

    @task(
        task_id="transform_data",
        execution_timeout=timedelta(minutes=5),
        inlets=[ASSET_BRONZE],
        outlets=[ASSET_SILVER],
    )
    def transform_data():
        """Transforma dados bronze: trata nulos, deriva colunas, salva silver."""
        print("=== INICIANDO TRANSFORMAÇÃO SILVER ===")

        input_path  = "/opt/nb/bronze/alunos_raw.csv"
        output_path = "/opt/nb/silver/alunos_transformado.csv"

        df = pd.read_csv(input_path)
        print(f"✅ Bronze lido: {len(df)} registros")

        # ── Tratamento de nulos ────────────────────────────────────────────
        for col in ["NOTA_MAT_1", "NOTA_MAT_2", "NOTA_MAT_3", "NOTA_MAT_4"]:
            if col in df.columns:
                df[col] = df[col].fillna(df[col].median())

        if "INGLES" in df.columns:
            df["INGLES"] = df["INGLES"].fillna(0).astype(int)

        # ── Colunas derivadas ──────────────────────────────────────────────
        nota_cols = ["NOTA_MAT_1", "NOTA_MAT_2", "NOTA_MAT_3", "NOTA_MAT_4"]
        df["MEDIA_GERAL"] = df[nota_cols].mean(axis=1).round(2)

        if "H_AULA_PRES" in df.columns and "FALTAS" in df.columns:
            total_aulas = df["H_AULA_PRES"] + df["FALTAS"]
            df["TAXA_PRESENCA"] = (
                df["H_AULA_PRES"] / total_aulas.replace(0, 1) * 100
            ).round(2)

        if "TAREFAS_ONLINE" in df.columns:
            df["INDICE_ENGAJAMENTO"] = (
                df.get("TAXA_PRESENCA", 0) * 0.6 + df["TAREFAS_ONLINE"] * 0.4
            ).round(2)

        reprov_cols = ["REPROVACOES_MAT_1", "REPROVACOES_MAT_2",
                       "REPROVACOES_MAT_3", "REPROVACOES_MAT_4"]
        df["TOTAL_REPROVACOES"] = df[reprov_cols].sum(axis=1)

        for i, col in enumerate(nota_cols, 1):
            df[f"STATUS_MAT_{i}"] = df[col].apply(
                lambda n: "Aprovado" if n >= 5 else "Reprovado"
            )

        df["data_transformacao"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        os.makedirs(os.path.dirname(output_path), exist_ok=True)
        df.to_csv(output_path, index=False)
        print(f"✅ Silver salvo: {output_path} — {len(df)} registros, {len(df.columns)} colunas")

        return f"✅ SILVER CONCLUÍDO: {len(df)} registros"

    transform_data()


silver_pipeline()
