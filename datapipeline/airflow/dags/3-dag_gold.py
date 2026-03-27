"""
DAG GOLD - Agregações e KPIs

INPUTS:  /opt/nb/silver/alunos_transformado.csv
OUTPUTS: /opt/nb/gold/kpis_dashboard.csv
         /opt/nb/gold/analise_risco.csv
         /opt/nb/gold/analise_engajamento.csv
         /opt/nb/gold/insights.csv

Linhagem no OpenMetadata:
  alunos_transformado ──► [3_gold_aggregation DAG] ──► kpis_dashboard
                                                    ──► analise_risco
                                                    ──► analise_engajamento
                                                    ──► insights
"""

from __future__ import annotations

import os
from datetime import datetime, timedelta

from airflow.sdk import dag, task, Asset
import pandas as pd

ASSET_SILVER       = Asset(uri="openmetadata://pipeline_alunos.educacao.camadas.alunos_transformado")
ASSET_KPIS         = Asset(uri="openmetadata://pipeline_alunos.educacao.camadas.kpis_dashboard")
ASSET_RISCO        = Asset(uri="openmetadata://pipeline_alunos.educacao.camadas.analise_risco")
ASSET_ENGAJAMENTO  = Asset(uri="openmetadata://pipeline_alunos.educacao.camadas.analise_engajamento")
ASSET_INSIGHTS     = Asset(uri="openmetadata://pipeline_alunos.educacao.camadas.insights")

GOLD_DIR = "/opt/nb/gold"


@dag(
    dag_id="3_gold_aggregation",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["gold", "aggregation", "kpi"],
    description="Agregações e KPIs na camada Gold",
    owner_links={"owner": "mailto:engenharia.dados@empresa.com"},
)
def gold_pipeline():

    @task(
        task_id="gerar_kpis",
        execution_timeout=timedelta(minutes=5),
        inlets=[ASSET_SILVER],
        outlets=[ASSET_KPIS],
    )
    def gerar_kpis():
        df = pd.read_csv("/opt/nb/silver/alunos_transformado.csv")
        os.makedirs(GOLD_DIR, exist_ok=True)

        kpis = [
            {"metrica": "total_alunos",          "valor": len(df),                                  "percentual": None,  "categoria": "Geral"},
            {"metrica": "media_geral_turma",      "valor": round(df["MEDIA_GERAL"].mean(), 2),       "percentual": None,  "categoria": "Desempenho"},
            {"metrica": "taxa_presenca_media",    "valor": round(df["TAXA_PRESENCA"].mean(), 2),     "percentual": None,  "categoria": "Frequência"},
            {"metrica": "indice_engajamento",     "valor": round(df["INDICE_ENGAJAMENTO"].mean(), 2),"percentual": None,  "categoria": "Engajamento"},
            {"metrica": "alunos_aprovados",       "valor": int((df["MEDIA_GERAL"] >= 5).sum()),      "percentual": round((df["MEDIA_GERAL"] >= 5).mean() * 100, 1), "categoria": "Desempenho"},
            {"metrica": "alunos_reprovados",      "valor": int((df["MEDIA_GERAL"] < 5).sum()),       "percentual": round((df["MEDIA_GERAL"] < 5).mean() * 100, 1),  "categoria": "Desempenho"},
            {"metrica": "alunos_risco_evasao",    "valor": int((df["TOTAL_REPROVACOES"] >= 3).sum()),"percentual": round((df["TOTAL_REPROVACOES"] >= 3).mean() * 100, 1), "categoria": "Risco"},
            {"metrica": "media_faltas",           "valor": round(df["FALTAS"].mean(), 2),            "percentual": None,  "categoria": "Frequência"},
            {"metrica": "tarefas_online_media",   "valor": round(df["TAREFAS_ONLINE"].mean(), 2),    "percentual": None,  "categoria": "Engajamento"},
            {"metrica": "total_reprovacoes_media","valor": round(df["TOTAL_REPROVACOES"].mean(), 2), "percentual": None,  "categoria": "Desempenho"},
            {"metrica": "alunos_sem_reprovacao",  "valor": int((df["TOTAL_REPROVACOES"] == 0).sum()),"percentual": round((df["TOTAL_REPROVACOES"] == 0).mean() * 100, 1), "categoria": "Desempenho"},
        ]
        pd.DataFrame(kpis).to_csv(f"{GOLD_DIR}/kpis_dashboard.csv", index=False)
        print(f"✅ KPIs gerados: {len(kpis)} métricas")

    @task(
        task_id="gerar_analise_risco",
        execution_timeout=timedelta(minutes=5),
        inlets=[ASSET_SILVER],
        outlets=[ASSET_RISCO],
    )
    def gerar_analise_risco():
        df = pd.read_csv("/opt/nb/silver/alunos_transformado.csv")
        os.makedirs(GOLD_DIR, exist_ok=True)

        total = len(df)
        risco_alto  = df[df["TOTAL_REPROVACOES"] >= 3]
        risco_medio = df[(df["TOTAL_REPROVACOES"] >= 1) & (df["TOTAL_REPROVACOES"] < 3)]
        sem_risco   = df[df["TOTAL_REPROVACOES"] == 0]
        baixa_pres  = df[df["TAXA_PRESENCA"] < 75]
        alto_eng    = df[df["INDICE_ENGAJAMENTO"] > 70]

        registros = [
            {"analise": "risco_alto",           "quantidade": len(risco_alto),  "percentual": round(len(risco_alto)/total*100,1),  "detalhes": "3+ reprovações"},
            {"analise": "risco_medio",          "quantidade": len(risco_medio), "percentual": round(len(risco_medio)/total*100,1), "detalhes": "1-2 reprovações"},
            {"analise": "sem_risco",            "quantidade": len(sem_risco),   "percentual": round(len(sem_risco)/total*100,1),   "detalhes": "Nenhuma reprovação"},
            {"analise": "baixa_presenca",       "quantidade": len(baixa_pres),  "percentual": round(len(baixa_pres)/total*100,1),  "detalhes": "Presença < 75%"},
            {"analise": "alto_engajamento",     "quantidade": len(alto_eng),    "percentual": round(len(alto_eng)/total*100,1),    "detalhes": "Índice > 70"},
            {"analise": "perfil_dificuldade",   "quantidade": int((df.get("PERFIL","") == "DIFICULDADE").sum()), "percentual": None, "detalhes": "Perfil DIFICULDADE"},
            {"analise": "perfil_desempenho",    "quantidade": int((df.get("PERFIL","") == "DESEMPENHO").sum()),  "percentual": None, "detalhes": "Perfil DESEMPENHO"},
        ]
        pd.DataFrame(registros).to_csv(f"{GOLD_DIR}/analise_risco.csv", index=False)
        print(f"✅ Análise de risco: {len(registros)} registros")

    @task(
        task_id="gerar_analise_engajamento",
        execution_timeout=timedelta(minutes=5),
        inlets=[ASSET_SILVER],
        outlets=[ASSET_ENGAJAMENTO],
    )
    def gerar_analise_engajamento():
        df = pd.read_csv("/opt/nb/silver/alunos_transformado.csv")
        os.makedirs(GOLD_DIR, exist_ok=True)

        registros = [
            {"analise": "engajamento_medio",        "valor": round(df["INDICE_ENGAJAMENTO"].mean(), 2),    "categoria": "Geral"},
            {"analise": "presenca_media",           "valor": round(df["TAXA_PRESENCA"].mean(), 2),         "categoria": "Presença"},
            {"analise": "tarefas_online_media",     "valor": round(df["TAREFAS_ONLINE"].mean(), 2),        "categoria": "Tarefas"},
            {"analise": "h_aula_pres_media",        "valor": round(df["H_AULA_PRES"].mean(), 2),           "categoria": "Presença"},
            {"analise": "alto_engajamento_pct",     "valor": round((df["INDICE_ENGAJAMENTO"] > 70).mean()*100, 1), "categoria": "Segmentação"},
            {"analise": "baixo_engajamento_pct",    "valor": round((df["INDICE_ENGAJAMENTO"] < 40).mean()*100, 1), "categoria": "Segmentação"},
            {"analise": "engajamento_mediano",      "valor": round(df["INDICE_ENGAJAMENTO"].median(), 2),  "categoria": "Distribuição"},
            {"analise": "engajamento_desvio_padrao","valor": round(df["INDICE_ENGAJAMENTO"].std(), 2),     "categoria": "Distribuição"},
        ]
        pd.DataFrame(registros).to_csv(f"{GOLD_DIR}/analise_engajamento.csv", index=False)
        print(f"✅ Análise de engajamento: {len(registros)} registros")

    @task(
        task_id="gerar_insights",
        execution_timeout=timedelta(minutes=5),
        inlets=[ASSET_SILVER],
        outlets=[ASSET_INSIGHTS],
    )
    def gerar_insights():
        df = pd.read_csv("/opt/nb/silver/alunos_transformado.csv")
        os.makedirs(GOLD_DIR, exist_ok=True)

        pct_risco   = round((df["TOTAL_REPROVACOES"] >= 3).mean() * 100, 1)
        pct_presenca = round(df["TAXA_PRESENCA"].mean(), 1)
        media_geral  = round(df["MEDIA_GERAL"].mean(), 2)
        pct_engaj    = round((df["INDICE_ENGAJAMENTO"] > 70).mean() * 100, 1)
        pct_semreprov = round((df["TOTAL_REPROVACOES"] == 0).mean() * 100, 1)
        pct_baixapres = round((df["TAXA_PRESENCA"] < 75).mean() * 100, 1)

        registros = [
            {"insight": "Alunos com alto risco de evasão",   "prioridade": "Alta",  "valor": pct_risco,    "detalhes": f"{pct_risco}% com 3+ reprovações"},
            {"insight": "Taxa de presença abaixo do ideal",  "prioridade": "Alta",  "valor": pct_baixapres,"detalhes": f"{pct_baixapres}% com presença < 75%"},
            {"insight": "Média geral da turma",              "prioridade": "Média", "valor": media_geral,  "detalhes": f"Média: {media_geral} de 10"},
            {"insight": "Alunos com alto engajamento",       "prioridade": "Média", "valor": pct_engaj,    "detalhes": f"{pct_engaj}% com índice > 70"},
            {"insight": "Presença média da turma",           "prioridade": "Baixa", "valor": pct_presenca, "detalhes": f"Presença média: {pct_presenca}%"},
            {"insight": "Alunos sem reprovações",            "prioridade": "Baixa", "valor": pct_semreprov,"detalhes": f"{pct_semreprov}% sem nenhuma reprovação"},
        ]
        pd.DataFrame(registros).to_csv(f"{GOLD_DIR}/insights.csv", index=False)
        print(f"✅ Insights gerados: {len(registros)} registros")

    # Todas as tasks gold rodam em paralelo a partir do silver
    gerar_kpis()
    gerar_analise_risco()
    gerar_analise_engajamento()
    gerar_insights()


gold_pipeline()
