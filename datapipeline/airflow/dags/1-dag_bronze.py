"""
DAG BRONZE - Ingestão de Dados Crus
Esta DAG simula o microsserviço de Ingestão gerando dados crus/normalizados 
e carregando-os na camada BRONZE.
INPUTS: /dados/curso.txt
OUTPUT: /dados/bronze/alunos_raw.csv
"""
from airflow.sdk import dag, task
from datetime import datetime, timedelta
import pandas as pd
import os

# LINEAGE BACKEND ADICIONADO
from airflow.lineage.entities import File


@dag(
    dag_id='1_bronze_ingestion',
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=['bronze', 'ingestion'],
    description='Ingestão de dados brutos na camada Bronze',
    owner_links={"owner": "mailto:engenharia.dados@empresa.com"}
)
def bronze_pipeline():

    # LINEAGE BACKEND ADICIONADO — outlets declaram o arquivo produzido por esta task
    @task(
        task_id='ingest_raw_data',
        execution_timeout=timedelta(minutes=2, seconds=30),
        outlets=[File(path="/opt/nb/bronze/alunos_raw.csv")],
    )
    def ingest_raw_data():
        """Ingere dados brutos do arquivo curso.txt para a camada Bronze"""
        print("=== INICIANDO INGESTÃO BRONZE ===")
        input_path = '/opt/nb/curso.txt'
        output_path = '/opt/nb/bronze/alunos_raw.csv'
        
        try:
            print(f"Lendo arquivo: {input_path}")
            df = pd.read_csv(input_path)
            print(f"✅ Arquivo lido: {len(df)} registros")
            
            # Adicionar metadados de ingestão
            df['data_ingestao'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            df['fonte'] = 'curso.txt'
            print("✅ Metadados adicionados")
            
            # Salvar na camada Bronze
            os.makedirs(os.path.dirname(output_path), exist_ok=True)
            df.to_csv(output_path, index=False)
            print(f"✅ Arquivo salvo: {output_path}")
            
            msg = f"✅ BRONZE CONCLUÍDO: {len(df)} registros salvos"
            print(msg)
            return msg
            
        except Exception as e:
            error_msg = f"❌ ERRO BRONZE: {str(e)}"
            print(error_msg)
            raise Exception(error_msg)

    ingest_raw_data()


bronze_pipeline()
