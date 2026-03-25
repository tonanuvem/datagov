"""
DAG TESTE SILVER - Validação da Camada Silver
Esta DAG testa a qualidade dos dados transformados na camada Silver.

INPUTS: /dados/silver/alunos_transformado.csv
OUTPUT: Logs de validação
"""
from airflow.sdk import dag, task
from datetime import datetime
import pandas as pd

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
        """Valida dados transformados da camada Silver"""
        print("=== INICIANDO TESTE SILVER ===")
        input_path = '/opt/nb/silver/alunos_transformado.csv'
        
        try:
            print(f"Lendo arquivo: {input_path}")
            df = pd.read_csv(input_path)
            print(f"✅ Arquivo lido: {len(df)} registros")
            
            tests = []
            
            # 1. Verificar se não há valores ausentes em campos críticos
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
            
            # 2. Verificar range de notas (0-10)
            print("Validando range de notas...")
            for col in ['NOTA_MAT_1', 'NOTA_MAT_2', 'NOTA_MAT_3', 'NOTA_MAT_4', 'MEDIA_GERAL']:
                invalid = ((df[col] < 0) | (df[col] > 10)).sum()
                resultado = "PASSOU" if invalid == 0 else "FALHOU"
                tests.append({
                    "teste": f"Range de {col} (0-10)",
                    "resultado": resultado,
                    "detalhes": f"{invalid} valores fora do range"
                })
                print(f"  - {col}: {resultado} ({invalid} fora do range)")
            
            # 3. Verificar se colunas derivadas foram criadas
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
            
            return result
            
        except Exception as e:
            error_msg = f"❌ ERRO TESTE SILVER: {str(e)}"
            print(error_msg)
            raise Exception(error_msg)
    
    validate_silver_data()

silver_test_pipeline()
