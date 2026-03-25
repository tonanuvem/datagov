"""
DAG TESTE GOLD - Validação da Camada Gold
Esta DAG testa a qualidade dos dados agregados na camada Gold.

INPUTS: /dados/gold/*.csv
OUTPUT: Logs de validação
"""
from airflow.sdk import dag, task
from datetime import datetime
import pandas as pd
import os

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
        """Valida dados agregados da camada Gold"""
        print("=== INICIANDO TESTE GOLD ===")
        gold_path = '/opt/nb/gold'
        
        try:
            expected_files = [
                'kpis_dashboard.csv',
                'analise_risco.csv',
                'analise_engajamento.csv',
                'insights.csv'
            ]
            
            tests = []
            
            # Verificar se todos os arquivos foram gerados
            print(f"Validando {len(expected_files)} arquivos esperados...")
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
                    print(f"  ✅ {file}: {len(df)} registros")
                else:
                    tests.append({
                        "arquivo": file,
                        "existe": "NAO",
                        "registros": 0,
                        "resultado": "FALHOU"
                    })
                    print(f"  ❌ {file}: não encontrado")
            
            gerados = sum(1 for t in tests if t['existe'] == 'SIM')
            
            result = {
                "arquivos_esperados": len(expected_files),
                "arquivos_gerados": gerados,
                "detalhes": tests
            }
            
            if gerados == len(expected_files):
                print(f"✅ TESTE GOLD PASSOU: {gerados}/{len(expected_files)} arquivos gerados")
            else:
                print(f"⚠️ TESTE GOLD: {gerados}/{len(expected_files)} arquivos gerados")
            
            return result
            
        except Exception as e:
            error_msg = f"❌ ERRO TESTE GOLD: {str(e)}"
            print(error_msg)
            raise Exception(error_msg)
    
    validate_gold_data()

gold_test_pipeline()
