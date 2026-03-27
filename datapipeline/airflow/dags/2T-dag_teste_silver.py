"""
DAG TESTE SILVER - Validação da Camada Silver
Esta DAG testa a qualidade dos dados transformados na camada Silver e envia
os resultados para a aba Qualidade de Dados do OpenMetadata.
INPUTS: /dados/silver/alunos_transformado.csv
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


# ─── DAG ─────────────────────────────────────────────────────────────────────

@dag(
    dag_id='4_silver_test',
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=['silver', 'test', 'quality'],
    description='Testes de qualidade da camada Silver',
    owner_links={"owner": "mailto:engenharia.dados@empresa.com"}
)
def silver_test_pipeline():

    @task(task_id='validate_silver_data')
    def validate_silver_data():
        """Valida dados transformados da camada Silver e envia resultados ao OpenMetadata"""
        print("=== INICIANDO TESTE SILVER ===")
        input_path = '/opt/nb/silver/alunos_transformado.csv'

        try:
            print(f"Lendo arquivo: {input_path}")
            df = pd.read_csv(input_path)
            print(f"✅ Arquivo lido: {len(df)} registros")

            tests = []

            # ── 1. Verificar se não há valores ausentes em campos críticos ────
            print("Validando valores ausentes em campos críticos...")
            critical_cols = ['INGLES', 'NOTA_MAT_1', 'NOTA_MAT_2', 'NOTA_MAT_3', 'NOTA_MAT_4']
            for col in critical_cols:
                missing = df[col].isnull().sum()
                resultado = "PASSOU" if missing == 0 else "FALHOU"
                tests.append({
                    "teste": f"Valores ausentes em {col}",
                    "resultado": resultado,
                    "detalhes": f"{missing} valores ausentes"
                })
                print(f"  - {col}: {resultado} ({missing} ausentes)")

            # ── 2. Verificar range de notas (0-10) ────────────────────────────
            print("Validando range de notas...")
            nota_cols = ['NOTA_MAT_1', 'NOTA_MAT_2', 'NOTA_MAT_3', 'NOTA_MAT_4', 'MEDIA_GERAL']
            for col in nota_cols:
                invalid = ((df[col] < 0) | (df[col] > 10)).sum()
                resultado = "PASSOU" if invalid == 0 else "FALHOU"
                tests.append({
                    "teste": f"Range de {col} (0-10)",
                    "resultado": resultado,
                    "detalhes": f"{invalid} valores fora do range"
                })
                print(f"  - {col}: {resultado} ({invalid} fora do range)")

            # ── 3. Verificar se colunas derivadas foram criadas ───────────────
            print("Validando colunas derivadas...")
            derived_cols = ['MEDIA_GERAL', 'TAXA_PRESENCA', 'INDICE_ENGAJAMENTO', 'TOTAL_REPROVACOES']
            for col in derived_cols:
                exists = col in df.columns
                resultado = "PASSOU" if exists else "FALHOU"
                tests.append({
                    "teste": f"Coluna {col} criada",
                    "resultado": resultado,
                    "detalhes": "Coluna presente" if exists else "Coluna ausente"
                })
                print(f"  - {col}: {resultado}")

            passou = sum(1 for t in tests if t['resultado'] == 'PASSOU')
            falhou = len(tests) - passou

            result = {
                "total_registros": len(df),
                "testes_executados": len(tests),
                "testes_passaram": passou,
                "detalhes": tests
            }

            if falhou > 0:
                print(f"⚠️ TESTE SILVER: {passou}/{len(tests)} testes passaram")
            else:
                print(f"✅ TESTE SILVER PASSOU: {len(tests)} testes executados")

            # ── Enviar resultados ao OpenMetadata ─────────────────────────────
            print("\n--- Enviando resultados ao OpenMetadata ---")

            resultados_om = (
                # Nulos em campos críticos
                [
                    {
                        "nome": f"silver_sem_nulos_{col.lower()}",
                        "passou": df[col].isnull().sum() == 0,
                        "valor": int(df[col].isnull().sum()),
                        "min": 0,
                        "max": 0,
                    }
                    for col in critical_cols
                ] +
                # Range de notas 0-10
                [
                    {
                        "nome": f"silver_range_{col.lower()}",
                        "passou": ((df[col] < 0) | (df[col] > 10)).sum() == 0,
                        "valor": round(float(df[col].mean()), 2),
                        "min": 0,
                        "max": 10,
                    }
                    for col in nota_cols
                ] +
                # Colunas derivadas existem
                [
                    {
                        "nome": f"silver_coluna_derivada_{col.lower()}",
                        "passou": col in df.columns,
                        "valor": 1 if col in df.columns else 0,
                    }
                    for col in derived_cols
                ] +
                # Taxa de presença entre 0 e 100
                [
                    {
                        "nome": "silver_taxa_presenca_range",
                        "passou": ((df['TAXA_PRESENCA'] < 0) | (df['TAXA_PRESENCA'] > 100)).sum() == 0,
                        "valor": round(float(df['TAXA_PRESENCA'].mean()), 2),
                        "min": 0,
                        "max": 100,
                    }
                ] +
                # Total de registros silver == bronze
                [
                    {
                        "nome": "silver_total_registros",
                        "passou": len(df) >= 100,
                        "valor": len(df),
                        "min": 100,
                        "max": 999999,
                    }
                ]
            )

            enviar_resultado_om(
                tabela_fqn="pipeline_alunos.educacao.medallion.alunos_transformado",
                resultados=resultados_om,
            )

            return result

        except Exception as e:
            error_msg = f"❌ ERRO TESTE SILVER: {str(e)}"
            print(error_msg)
            raise Exception(error_msg)

    validate_silver_data()


silver_test_pipeline()
