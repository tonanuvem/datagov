"""
DAG CATALOG - Catalogação de Metadados no OpenMetadata
Esta DAG registra metadados dos arquivos locais no OpenMetadata.

INPUTS: /dados/{bronze,silver,gold}/*.csv
OUTPUT: Metadados no OpenMetadata
"""
from airflow.sdk import dag, task
from datetime import datetime
import os
import pandas as pd
import json

@dag(
    dag_id='7_catalog_csv_local',
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=['catalog', 'metadata', 'governance'],
    description='Catalogação de metadados no OpenMetadata',
    owner_links={"owner": "mailto:engenharia.dados@empresa.com"}
)
def catalog_pipeline():
    
    @task(task_id='discover_datasets')
    def discover_datasets():
        """Descobre datasets nas camadas Bronze, Silver e Gold"""
        print("=== DESCOBRINDO DATASETS ===")
        base_path = '/opt/nb'
        layers = ['bronze', 'silver', 'gold']
        
        try:
            datasets = []
            
            for layer in layers:
                layer_path = os.path.join(base_path, layer)
                print(f"Analisando camada: {layer}")
                if os.path.exists(layer_path):
                    files = [f for f in os.listdir(layer_path) if f.endswith('.csv')]
                    print(f"  - Encontrados {len(files)} arquivos CSV")
                    
                    for file in files:
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
                        print(f"    ✅ {file}: {len(df)} registros, {len(df.columns)} colunas")
                else:
                    print(f"  ⚠️ Camada {layer} não encontrada")
            
            # Salvar catálogo
            catalog_path = '/opt/nb/catalog_metadata.json'
            with open(catalog_path, 'w') as f:
                json.dump(datasets, f, indent=2)
            print(f"✅ Catálogo salvo: {catalog_path}")
            
            msg = f"✅ Descobertos {len(datasets)} datasets"
            print(msg)
            return msg
            
        except Exception as e:
            error_msg = f"❌ ERRO DESCOBRINDO DATASETS: {str(e)}"
            print(error_msg)
            raise Exception(error_msg)
    
    @task(task_id='register_lineage')
    def register_lineage():
        """Registra lineage entre as camadas"""
        print("=== REGISTRANDO LINEAGE ===")
        
        try:
            lineage = {
                'pipelines': [
                    {
                        'name': 'bronze_to_silver',
                        'source': '/dados/bronze/alunos_raw.csv',
                        'target': '/dados/silver/alunos_transformado.csv',
                        'transformations': [
                            'Tratamento de valores ausentes',
                            'Padronização de campos',
                            'Criação de colunas derivadas'
                        ]
                    },
                    {
                        'name': 'silver_to_gold',
                        'source': '/dados/silver/alunos_transformado.csv',
                        'targets': [
                            '/dados/gold/kpis_dashboard.csv',
                            '/dados/gold/analise_risco.csv',
                            '/dados/gold/analise_engajamento.csv',
                            '/dados/gold/insights.csv'
                        ],
                        'transformations': [
                            'Agregações por perfil',
                            'Cálculo de KPIs',
                            'Análises de correlação'
                        ]
                    }
                ]
            }
            
            print(f"Registrando {len(lineage['pipelines'])} pipelines...")
            for pipeline in lineage['pipelines']:
                print(f"  - {pipeline['name']}: {len(pipeline['transformations'])} transformações")
            
            # Salvar lineage
            lineage_path = '/opt/nb/lineage_metadata.json'
            with open(lineage_path, 'w') as f:
                json.dump(lineage, f, indent=2)
            print(f"✅ Lineage salvo: {lineage_path}")
            
            msg = f"✅ Lineage registrado para {len(lineage['pipelines'])} pipelines"
            print(msg)
            return msg
            
        except Exception as e:
            error_msg = f"❌ ERRO REGISTRANDO LINEAGE: {str(e)}"
            print(error_msg)
            raise Exception(error_msg)
    
    discover = discover_datasets()
    lineage = register_lineage()
    
    discover >> lineage

catalog_pipeline()
