"""
DAG TESTE GOLD - Validação da Camada Gold
Esta DAG testa a qualidade dos dados agregados na camada Gold e envia
os resultados para a aba Qualidade de Dados do OpenMetadata.
INPUTS: /dados/gold/*.csv
OUTPUT: Logs de validação + resultados no OpenMetadata
"""
from airflow.sdk import dag, task
from datetime import datetime
import pandas as pd
import os


# ─── Helper: envia resultados para o OpenMetadata ────────────────────────────

def enviar_resultado_om(tabela_fqn: str, resultados: list):
    """
    Envia resultados de testes para a aba Qualidade de Dados do OpenMetadata.
    resultados: lista de dicts com keys: nome, passou, valor, min (opcional), max (opcional)
    """
    try:
        from metadata.generated.schema.entity.services.connections.metadata.openMetadataConnection import (
            OpenMetadataConnection,
        )
        from metadata.generated.schema.security.client.openMetadataJWTClientConfig import (
            OpenMetadataJWTClientConfig,
        )
        from metadata.ingestion.ometa.ometa_api import OpenMetadata
        from metadata.generated.schema.api.tests.createTestSuite import CreateTestSuiteRequest
        from metadata.generated.schema.api.tests.createTestCase import CreateTestCaseRequest
        from metadata.generated.schema.tests.testCase import TestCaseParameterValue
        from metadata.generated.schema.tests.testSuite import TestSuite
        from metadata.generated.schema.tests.basic import TestCaseStatus, TestResultValue

        metadata = OpenMetadata(OpenMetadataConnection(
            hostPort="http://openmetadata-server:8585/api",
            authProvider="openmetadata",
            securityConfig=OpenMetadataJWTClientConfig(
                jwtToken=os.getenv("OPENMETADATA_JWT_TOKEN")
            ),
        ))

        suite_nome = f"{tabela_fqn}.testSuite"
        try:
            suite = metadata.get_by_name(entity=TestSuite, fqn=suite_nome)
            if not suite:
                suite = metadata.create_or_update(CreateTestSuiteRequest(
                    name=suite_nome,
                    executableEntityReference=tabela_fqn,
                ))
                print(f"✅ TestSuite criada: {suite_nome}")
            else:
                print(f"✅ TestSuite já existe: {suite_nome}")
        except Exception as e:
            print(f"⚠️ Erro ao criar TestSuite: {e}")
            return

        for r in resultados:
            try:
                params = []
                if "min" in r:
                    params = [
                        TestCaseParameterValue(name="minValue", value=str(r["min"])),
                        TestCaseParameterValue(name="maxValue", value=str(r["max"])),
                    ]

                metadata.create_or_update(CreateTestCaseRequest(
                    name=r["nome"],
                    testSuite=suite_nome,
                    entityLink=f"<#E::table::{tabela_fqn}>",
                    testDefinition="columnValuesToBeBetween" if params else "tableRowCountToEqual",
                    parameterValues=params,
                ))

                status = TestCaseStatus.Success if r["passou"] else TestCaseStatus.Failed
                metadata.add_test_case_results(
                    test_case_fqn=f"{suite_nome}.{r['nome']}",
                    result=status,
                    timestamp=int(datetime.now().timestamp() * 1000),
                    testResultValue=[TestResultValue(
                        value=str(r.get("valor", "")),
                        name=r["nome"],
                    )],
                )
                icone = "✅" if r["passou"] else "❌"
                print(f"  {icone} OM Qualidade registrado: {r['nome']} → {status}")

            except Exception as e:
                print(f"⚠️ Não foi possível enviar resultado {r['nome']} ao OM: {e}")

    except ImportError as e:
        print(f"⚠️ SDK OpenMetadata não disponível, pulando envio de qualidade: {e}")


# ─── Configuração dos arquivos gold esperados ─────────────────────────────────

GOLD_FILES = {
    "kpis_dashboard.csv":      {"tabela": "kpis_dashboard",      "min_rows": 5},
    "analise_risco.csv":       {"tabela": "analise_risco",        "min_rows": 3},
    "analise_engajamento.csv": {"tabela": "analise_engajamento",  "min_rows": 3},
    "insights.csv":            {"tabela": "insights",             "min_rows": 3},
}


# ─── DAG ─────────────────────────────────────────────────────────────────────

@dag(
    dag_id='6_gold_test',
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=['gold', 'test', 'quality'],
    description='Testes de qualidade da camada Gold',
    owner_links={"owner": "mailto:engenharia.dados@empresa.com"}
)
def gold_test_pipeline():

    @task(task_id='validate_gold_data')
    def validate_gold_data():
        """Valida dados agregados da camada Gold e envia resultados ao OpenMetadata"""
        print("=== INICIANDO TESTE GOLD ===")
        gold_path = '/opt/nb/gold'

        try:
            tests = []

            # ── Verificar se todos os arquivos foram gerados ──────────────────
            print(f"Validando {len(GOLD_FILES)} arquivos esperados...")
            for file, cfg in GOLD_FILES.items():
                file_path = os.path.join(gold_path, file)
                exists = os.path.exists(file_path)

                if exists:
                    df = pd.read_csv(file_path)
                    tests.append({
                        "arquivo":   file,
                        "tabela":    cfg["tabela"],
                        "existe":    "SIM",
                        "registros": len(df),
                        "resultado": "PASSOU",
                        "df":        df,
                        "min_rows":  cfg["min_rows"],
                    })
                    print(f"  ✅ {file}: {len(df)} registros")
                else:
                    tests.append({
                        "arquivo":   file,
                        "tabela":    cfg["tabela"],
                        "existe":    "NAO",
                        "registros": 0,
                        "resultado": "FALHOU",
                        "df":        None,
                        "min_rows":  cfg["min_rows"],
                    })
                    print(f"  ❌ {file}: não encontrado")

            gerados = sum(1 for t in tests if t["existe"] == "SIM")

            result = {
                "arquivos_esperados": len(GOLD_FILES),
                "arquivos_gerados":   gerados,
                "detalhes": [
                    {k: v for k, v in t.items() if k != "df"}
                    for t in tests
                ],
            }

            if gerados == len(GOLD_FILES):
                print(f"✅ TESTE GOLD PASSOU: {gerados}/{len(GOLD_FILES)} arquivos gerados")
            else:
                print(f"⚠️ TESTE GOLD: {gerados}/{len(GOLD_FILES)} arquivos gerados")

            # ── Enviar resultados ao OpenMetadata — um TestSuite por tabela ───
            print("\n--- Enviando resultados ao OpenMetadata ---")
            for t in tests:
                fqn = f"pipeline_alunos.educacao.medallion.{t['tabela']}"
                resultados_om = [
                    # Arquivo existe
                    {
                        "nome":   f"gold_{t['tabela']}_arquivo_gerado",
                        "passou": t["existe"] == "SIM",
                        "valor":  1 if t["existe"] == "SIM" else 0,
                    },
                    # Número mínimo de registros
                    {
                        "nome":   f"gold_{t['tabela']}_min_registros",
                        "passou": t["registros"] >= t["min_rows"],
                        "valor":  t["registros"],
                        "min":    t["min_rows"],
                        "max":    999999,
                    },
                ]

                # Testes específicos por tabela
                if t["df"] is not None:
                    df = t["df"]

                    if t["tabela"] == "kpis_dashboard":
                        # Valores numéricos não negativos
                        invalidos = (df["valor"] < 0).sum()
                        resultados_om.append({
                            "nome":   "gold_kpis_valores_nao_negativos",
                            "passou": invalidos == 0,
                            "valor":  int(invalidos),
                            "min":    0,
                            "max":    0,
                        })
                        # Categorias preenchidas
                        resultados_om.append({
                            "nome":   "gold_kpis_categoria_preenchida",
                            "passou": df["categoria"].isnull().sum() == 0,
                            "valor":  int(df["categoria"].isnull().sum()),
                        })

                    elif t["tabela"] == "analise_risco":
                        # Percentuais entre 0 e 100
                        pct_validos = df["percentual"].dropna()
                        invalidos = ((pct_validos < 0) | (pct_validos > 100)).sum()
                        resultados_om.append({
                            "nome":   "gold_risco_percentual_range",
                            "passou": invalidos == 0,
                            "valor":  int(invalidos),
                            "min":    0,
                            "max":    100,
                        })

                    elif t["tabela"] == "analise_engajamento":
                        # Sem nulos em coluna análise
                        resultados_om.append({
                            "nome":   "gold_engajamento_analise_preenchida",
                            "passou": df["analise"].isnull().sum() == 0,
                            "valor":  int(df["analise"].isnull().sum()),
                        })

                    elif t["tabela"] == "insights":
                        # Prioridades válidas
                        prioridades_validas = {"ALTA", "MEDIA", "BAIXA"}
                        invalidas = (~df["prioridade"].isin(prioridades_validas)).sum()
                        resultados_om.append({
                            "nome":   "gold_insights_prioridade_valida",
                            "passou": invalidas == 0,
                            "valor":  int(invalidas),
                        })

                enviar_resultado_om(tabela_fqn=fqn, resultados=resultados_om)

            return result

        except Exception as e:
            error_msg = f"❌ ERRO TESTE GOLD: {str(e)}"
            print(error_msg)
            raise Exception(error_msg)

    validate_gold_data()


gold_test_pipeline()
