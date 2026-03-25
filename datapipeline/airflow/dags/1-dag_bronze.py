"""
DAG BRONZE - Ingestão de Dados Crus
Esta DAG simula o microsserviço de Ingestão gerando dados crus/normalizados 
e carregando-os na camada BRONZE.

INPUTS: /ml/curso.txt
OUTPUT: /ml/data/bronze/alunos_raw.csv
"""
from airflow.decorators import dag, task
from datetime import datetime
import pandas as pd
import os

@dag(
    dag_id='bronze_ingestion',
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=['bronze', 'ingestion'],
    description='Ingestão de dados brutos na camada Bronze',
    owner_links={"owner": "mailto:engenharia.dados@empresa.com"}
)
def bronze_pipeline():
    
    @task(task_id='ingest_raw_data')
    def ingest_raw_data():
        """Ingere dados brutos do arquivo curso.txt para a camada Bronze"""
        input_path = '/opt/nb/curso.txt'
        output_path = '/opt/nb/data/bronze/alunos_raw.csv'
        
        # Ler dados brutos
        df = pd.read_csv(input_path)
        
        # Adicionar metadados de ingestão
        df['data_ingestao'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        df['fonte'] = 'curso.txt'
        
        # Salvar na camada Bronze
        os.makedirs(os.path.dirname(output_path), exist_ok=True)
        df.to_csv(output_path, index=False)
        
        return f"Ingestão concluída: {len(df)} registros salvos em {output_path}"
    
    ingest_raw_data()

bronze_pipeline()
