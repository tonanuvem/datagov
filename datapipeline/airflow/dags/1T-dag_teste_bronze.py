"""
DAG TESTE BRONZE - Validação da Camada Bronze
Esta DAG testa a qualidade dos dados na camada Bronze.

INPUTS: /dados/bronze/alunos_raw.csv
OUTPUT: Logs de validação
"""
from airflow.sdk import dag, task
from datetime import datetime
import pandas as pd

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
        """Valida dados da camada Bronze"""
        print("=== INICIANDO TESTE BRONZE ===")
        input_path = '/opt/nb/bronze/alunos_raw.csv'
        
        try:
            print(f"Lendo arquivo: {input_path}")
            df = pd.read_csv(input_path)
            print(f"✅ Arquivo lido: {len(df)} registros")
            
            # Testes básicos
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
            return result
            
        except Exception as e:
            error_msg = f"❌ ERRO TESTE BRONZE: {str(e)}"
            print(error_msg)
            raise Exception(error_msg)
    
    validate_bronze_data()

bronze_test_pipeline()
