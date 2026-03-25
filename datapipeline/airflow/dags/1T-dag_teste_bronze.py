"""
DAG TESTE BRONZE - Validação da Camada Bronze
Esta DAG testa a qualidade dos dados na camada Bronze.

INPUTS: /ml/data/bronze/alunos_raw.csv
OUTPUT: Logs de validação
"""
from airflow.decorators import dag, task
from datetime import datetime
import pandas as pd

@dag(
    dag_id='bronze_test',
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=['bronze', 'test', 'quality'],
    description='Testes de qualidade da camada Bronze',
    owner_links={"owner": "mailto:engenharia.dados@empresa.com"}
)
def bronze_test_pipeline():
    
    @task(task_id='validate_bronze_data')
    def validate_bronze_data():
        """Valida dados da camada Bronze"""
        input_path = '/opt/nb/data/bronze/alunos_raw.csv'
        
        df = pd.read_csv(input_path)
        
        # Testes básicos
        assert len(df) > 0, "Dataset vazio"
        assert 'MATRICULA' in df.columns, "Coluna MATRICULA ausente"
        assert 'PERFIL' in df.columns, "Coluna PERFIL ausente"
        
        # Verificar valores ausentes
        missing = df.isnull().sum()
        
        return {
            "total_registros": len(df),
            "colunas": list(df.columns),
            "valores_ausentes": missing[missing > 0].to_dict(),
            "status": "PASSOU"
        }
    
    validate_bronze_data()

bronze_test_pipeline()
