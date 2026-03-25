"""
DAG TESTE GOLD - Validação da Camada Gold
Esta DAG testa a qualidade dos dados agregados na camada Gold.

INPUTS: /ml/data/gold/*.csv
OUTPUT: Logs de validação
"""
from airflow.decorators import dag, task
from datetime import datetime
import pandas as pd
import os

@dag(
    dag_id='gold_test',
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=['gold', 'test', 'quality'],
    description='Testes de qualidade da camada Gold',
    owner_links={"owner": "mailto:engenharia.dados@empresa.com"}
)
def gold_test_pipeline():
    
    @task(task_id='validate_gold_data')
    def validate_gold_data():
        """Valida dados agregados da camada Gold"""
        gold_path = '/opt/nb/data/gold'
        
        expected_files = [
            'kpis_dashboard.csv',
            'analise_risco.csv',
            'analise_engajamento.csv',
            'insights.csv'
        ]
        
        tests = []
        
        # Verificar se todos os arquivos foram gerados
        for file in expected_files:
            file_path = os.path.join(gold_path, file)
            exists = os.path.exists(file_path)
            
            if exists:
                df = pd.read_csv(file_path)
                tests.append({
                    "arquivo": file,
                    "existe": "SIM",
                    "registros": len(df),
                    "resultado": "PASSOU"
                })
            else:
                tests.append({
                    "arquivo": file,
                    "existe": "NAO",
                    "registros": 0,
                    "resultado": "FALHOU"
                })
        
        return {
            "arquivos_esperados": len(expected_files),
            "arquivos_gerados": sum(1 for t in tests if t['existe'] == 'SIM'),
            "detalhes": tests
        }
    
    validate_gold_data()

gold_test_pipeline()
