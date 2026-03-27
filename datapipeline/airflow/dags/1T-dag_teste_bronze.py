"""
DAG TESTE BRONZE - Validação da Camada Bronze
Esta DAG testa a qualidade dos dados na camada Bronze e envia os resultados
para a aba Qualidade de Dados do OpenMetadata.
INPUTS: /dados/bronze/alunos_raw.csv
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

        # Criar ou obter TestSuite vinculada à tabela
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

        # Criar cada test case e registrar resultado
        for r in resultados:
            try:
                # Montar parâmetros se for teste de range
                params = []
                if "min" in r:
                    params = [
                        TestCaseParameterValue(name="minValue", value=str(r["min"])),
                        TestCaseParameterValue(name="maxValue", value=str(r["max"])),
                    ]

                # Criar ou atualizar o test case
                metadata.create_or_update(CreateTestCaseRequest(
                    name=r["nome"],
                    testSuite=suite_nome,
                    entityLink=f"<#E::table::{tabela_fqn}>",
                    testDefinition="columnValuesToBeBetween" if params else "tableRowCountToEqual",
                    parameterValues=params,
                ))

                # Registrar resultado
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
    dag_id='2_bronze_test',
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=['bronze', 'test', 'quality'],
    description='Testes de qualidade da camada Bronze',
    owner_links={"owner": "mailto:engenharia.dados@empresa.com"}
)
def bronze_test_pipeline():

    @task(task_id='validate_bronze_data')
    def validate_bronze_data():
        """Valida dados da camada Bronze e envia resultados ao OpenMetadata"""
        print("=== INICIANDO TESTE BRONZE ===")
        input_path = '/opt/nb/bronze/alunos_raw.csv'

        try:
            print(f"Lendo arquivo: {input_path}")
            df = pd.read_csv(input_path)
            print(f"✅ Arquivo lido: {len(df)} registros")

            # ── Testes existentes ─────────────────────────────────────────────
            assert len(df) > 0, "Dataset vazio"
            print("✅ Dataset não está vazio")

            assert 'MATRICULA' in df.columns, "Coluna MATRICULA ausente"
            print("✅ Coluna MATRICULA presente")

            assert 'PERFIL' in df.columns, "Coluna PERFIL ausente"
            print("✅ Coluna PERFIL presente")

            # Verificar valores ausentes
            missing = df.isnull().sum()
            print(f"Valores ausentes: {missing[missing > 0].to_dict()}")

            result = {
                "total_registros": len(df),
                "colunas": list(df.columns),
                "valores_ausentes": missing[missing > 0].to_dict(),
                "status": "PASSOU"
            }

            print(f"✅ TESTE BRONZE PASSOU: {len(df)} registros validados")

            # ── Enviar resultados ao OpenMetadata ─────────────────────────────
            print("\n--- Enviando resultados ao OpenMetadata ---")
            colunas_obrigatorias = [
                'MATRICULA', 'NOME', 'PERFIL', 'FALTAS',
                'H_AULA_PRES', 'TAREFAS_ONLINE', 'data_ingestao', 'fonte'
            ]
            resultados_om = [
                {
                    "nome": "bronze_dataset_nao_vazio",
                    "passou": len(df) > 0,
                    "valor": len(df),
                    "min": 1,
                    "max": 999999,
                },
                {
                    "nome": "bronze_total_registros_minimo",
                    "passou": len(df) >= 100,
                    "valor": len(df),
                    "min": 100,
                    "max": 999999,
                },
            ] + [
                {
                    "nome": f"bronze_coluna_{col.lower()}_presente",
                    "passou": col in df.columns,
                    "valor": 1 if col in df.columns else 0,
                }
                for col in colunas_obrigatorias
            ] + [
                {
                    "nome": f"bronze_nulos_{col.lower()}",
                    "passou": int(missing.get(col, 0)) == 0,
                    "valor": int(missing.get(col, 0)),
                    "min": 0,
                    "max": 0,
                }
                for col in ['MATRICULA', 'NOME', 'PERFIL']
            ]

            enviar_resultado_om(
                tabela_fqn="pipeline_alunos.educacao.medallion.alunos_raw",
                resultados=resultados_om,
            )

            return result

        except Exception as e:
            error_msg = f"❌ ERRO TESTE BRONZE: {str(e)}"
            print(error_msg)
            raise Exception(error_msg)

    validate_bronze_data()


bronze_test_pipeline()
