"""
DAG SILVER - Transformação e Limpeza de Dados
Esta DAG representa o microsserviço Correlator & ETL. Ela consome o csv da BRONZE, 
executa as transformações e correlações, e gera o fato consolidado na camada SILVER.

INPUTS: /dados/bronze/alunos_raw.csv
OUTPUT: /dados/silver/alunos_transformado.csv
"""
from airflow.sdk import dag, task
from datetime import datetime
import pandas as pd
import numpy as np
import os

@dag(
    dag_id='3_silver_transformation',
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=['silver', 'transformation', 'etl'],
    description='Transformação e limpeza de dados na camada Silver',
    owner_links={"owner": "mailto:engenharia.dados@empresa.com"}
)
def silver_pipeline():
    
    @task(task_id='transform_data')
    def transform_data():
        """Executa transformações e limpeza dos dados"""
        print("=== INICIANDO TRANSFORMAÇÃO SILVER ===")
        input_path = '/opt/nb/bronze/alunos_raw.csv'
        output_path = '/opt/nb/silver/alunos_transformado.csv'
        
        try:
            print(f"Lendo arquivo: {input_path}")
            df = pd.read_csv(input_path)
            print(f"✅ Arquivo lido: {len(df)} registros")
            
            # 1. Tratamento de Valores Ausentes
            print("Tratando valores ausentes...")
            df['INGLES'] = df['INGLES'].fillna(0)
            
            for col in ['NOTA_MAT_1', 'NOTA_MAT_2', 'NOTA_MAT_3', 'NOTA_MAT_4']:
                missing_count = df[col].isnull().sum()
                if missing_count > 0:
                    print(f"  - {col}: {missing_count} valores ausentes preenchidos com mediana")
                df[col] = df[col].fillna(df[col].median())
            print("✅ Valores ausentes tratados")
            
            # 2. Padronização
            print("Padronizando campos...")
            df['INGLES'] = df['INGLES'].astype(int)
            print("✅ Campo INGLES padronizado")
            
            # 3. Validação de Consistência
            print("Validando consistência de reprovações...")
            ajustes = 0
            for i in range(1, 5):
                reprov_col = f'REPROVACOES_MAT_{i}'
                nota_col = f'NOTA_MAT_{i}'
                ajustes += (df[reprov_col] > 0).sum()
                df.loc[df[reprov_col] > 0, nota_col] = 0
            print(f"✅ {ajustes} registros ajustados por consistência")
            
            # 4. Enriquecimento - Criar colunas derivadas
            print("Criando colunas derivadas...")
            df['MEDIA_GERAL'] = df[['NOTA_MAT_1', 'NOTA_MAT_2', 'NOTA_MAT_3', 'NOTA_MAT_4']].mean(axis=1).round(2)
            
            # Taxa de presença (assumindo 20 aulas totais)
            df['TAXA_PRESENCA'] = ((20 - df['FALTAS']) / 20 * 100).round(2)
            
            # Índice de engajamento (combinação de presença e tarefas)
            df['INDICE_ENGAJAMENTO'] = ((df['H_AULA_PRES'] * 2 + df['TAREFAS_ONLINE']) / 3).round(2)
            
            # Total de reprovações
            df['TOTAL_REPROVACOES'] = df[['REPROVACOES_MAT_1', 'REPROVACOES_MAT_2', 
                                           'REPROVACOES_MAT_3', 'REPROVACOES_MAT_4']].sum(axis=1)
            print("✅ Colunas derivadas criadas: MEDIA_GERAL, TAXA_PRESENCA, INDICE_ENGAJAMENTO, TOTAL_REPROVACOES")
            
            # Status por matéria (cursou ou não)
            print("Calculando status por matéria...")
            for i in range(1, 5):
                nota_col = f'NOTA_MAT_{i}'
                reprov_col = f'REPROVACOES_MAT_{i}'
                status_col = f'STATUS_MAT_{i}'
                df[status_col] = df.apply(
                    lambda row: 'NAO_CURSOU' if pd.isna(row[nota_col]) or row[nota_col] == 0 
                    else 'REPROVADO' if row[reprov_col] > 0 
                    else 'APROVADO', axis=1
                )
            print("✅ Status por matéria calculado")
            
            # Adicionar timestamp de transformação
            df['data_transformacao'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            
            # Salvar na camada Silver
            os.makedirs(os.path.dirname(output_path), exist_ok=True)
            df.to_csv(output_path, index=False)
            print(f"✅ Arquivo salvo: {output_path}")
            
            msg = f"✅ SILVER CONCLUÍDO: {len(df)} registros transformados"
            print(msg)
            return msg
            
        except Exception as e:
            error_msg = f"❌ ERRO SILVER: {str(e)}"
            print(error_msg)
            raise Exception(error_msg)
    
    transform_data()

silver_pipeline()
