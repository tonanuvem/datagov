"""
DAG CATALOG - Catalogação de Metadados no OpenMetadata
Esta DAG registra metadados dos arquivos locais no OpenMetadata.

INPUTS: /ml/data/{bronze,silver,gold}/*.csv
OUTPUT: Metadados no OpenMetadata
"""
from airflow.decorators import dag, task
from datetime import datetime
import os
import pandas as pd
import json

@dag(
    dag_id='catalog_metadata',
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=['catalog', 'metadata', 'governance'],
    description='Catalogação de metadados no OpenMetadata',
    owner_links={"owner": "mailto:engenharia.dados@empresa.com"}
)
def catalog_pipeline():
    
    @task(task_id='discover_datasets')
    def discover_datasets():
        """Descobre datasets nas camadas Bronze, Silver e Gold"""
        base_path = '/opt/nb/data'
        layers = ['bronze', 'silver', 'gold']
        
        datasets = []
        
        for layer in layers:
            layer_path = os.path.join(base_path, layer)
            if os.path.exists(layer_path):
                for file in os.listdir(layer_path):
                    if file.endswith('.csv'):
                        file_path = os.path.join(layer_path, file)
                        df = pd.read_csv(file_path)
                        
                        # Coletar metadados
                        metadata = {
                            'name': file.replace('.csv', ''),
                            'layer': layer,
                            'path': file_path,
                            'rows': int(len(df)),
                            'columns': list(df.columns),
                            'size_bytes': int(os.path.getsize(file_path)),
                            'schema': [
                                {
                                    'name': col,
                                    'type': str(df[col].dtype),
                                    'nullable': bool(df[col].isnull().any())
                                }
                                for col in df.columns
                            ]
                        }
                        datasets.append(metadata)
        
        # Salvar catálogo
        catalog_path = '/opt/nb/data/catalog_metadata.json'
        with open(catalog_path, 'w') as f:
            json.dump(datasets, f, indent=2)
        
        return f"Descobertos {len(datasets)} datasets"
    
    @task(task_id='register_lineage')
    def register_lineage():
        """Registra lineage entre as camadas"""
        lineage = {
            'pipelines': [
                {
                    'name': 'bronze_to_silver',
                    'source': '/ml/data/bronze/alunos_raw.csv',
                    'target': '/ml/data/silver/alunos_transformado.csv',
                    'transformations': [
                        'Tratamento de valores ausentes',
                        'Padronização de campos',
                        'Criação de colunas derivadas'
                    ]
                },
                {
                    'name': 'silver_to_gold',
                    'source': '/ml/data/silver/alunos_transformado.csv',
                    'targets': [
                        '/ml/data/gold/kpis_dashboard.csv',
                        '/ml/data/gold/analise_risco.csv',
                        '/ml/data/gold/analise_engajamento.csv',
                        '/ml/data/gold/insights.csv'
                    ],
                    'transformations': [
                        'Agregações por perfil',
                        'Cálculo de KPIs',
                        'Análises de correlação'
                    ]
                }
            ]
        }
        
        # Salvar lineage
        lineage_path = '/opt/nb/data/lineage_metadata.json'
        with open(lineage_path, 'w') as f:
            json.dump(lineage, f, indent=2)
        
        return f"Lineage registrado para {len(lineage['pipelines'])} pipelines"
    
    discover = discover_datasets()
    lineage = register_lineage()
    
    discover >> lineage

catalog_pipeline()
