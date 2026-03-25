"""
DAG SILVER - Transformação e Limpeza de Dados
Esta DAG representa o microsserviço Correlator & ETL. Ela consome o csv da BRONZE, 
executa as transformações e correlações, e gera o fato consolidado na camada SILVER.

INPUTS: /ml/data/bronze/alunos_raw.csv
OUTPUT: /ml/data/silver/alunos_transformado.csv
"""
from airflow.decorators import dag, task
from datetime import datetime
import pandas as pd
import numpy as np
import os

@dag(
    dag_id='silver_transformation',
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=['silver', 'transformation', 'etl'],
    description='Transformação e limpeza de dados na camada Silver',
    owner_links={"owner": "mailto:engenharia.dados@empresa.com"}
)
def silver_pipeline():
    
    @task(task_id='transform_data')
    def transform_data():
        """Executa transformações e limpeza dos dados"""
        input_path = '/opt/nb/data/bronze/alunos_raw.csv'
        output_path = '/opt/nb/data/silver/alunos_transformado.csv'
        
        df = pd.read_csv(input_path)
        
        # 1. Tratamento de Valores Ausentes
        # INGLES: preencher com 0 (não tem inglês)
        df['INGLES'] = df['INGLES'].fillna(0)
        
        # NOTAS: preencher com mediana da respectiva matéria
        for col in ['NOTA_MAT_1', 'NOTA_MAT_2', 'NOTA_MAT_3', 'NOTA_MAT_4']:
            df[col] = df[col].fillna(df[col].median())
        
        # 2. Padronização
        # INGLES: converter para booleano (0/1)
        df['INGLES'] = df['INGLES'].astype(int)
        
        # 3. Validação de Consistência
        # Verificar se alunos com reprovações têm nota 0
        for i in range(1, 5):
            reprov_col = f'REPROVACOES_MAT_{i}'
            nota_col = f'NOTA_MAT_{i}'
            # Se tem reprovação mas nota > 0, ajustar
            df.loc[df[reprov_col] > 0, nota_col] = 0
        
        # 4. Enriquecimento - Criar colunas derivadas
        # Média geral de notas
        df['MEDIA_GERAL'] = df[['NOTA_MAT_1', 'NOTA_MAT_2', 'NOTA_MAT_3', 'NOTA_MAT_4']].mean(axis=1).round(2)
        
        # Taxa de presença (assumindo 20 aulas totais)
        df['TAXA_PRESENCA'] = ((20 - df['FALTAS']) / 20 * 100).round(2)
        
        # Índice de engajamento (combinação de presença e tarefas)
        df['INDICE_ENGAJAMENTO'] = ((df['H_AULA_PRES'] * 2 + df['TAREFAS_ONLINE']) / 3).round(2)
        
        # Total de reprovações
        df['TOTAL_REPROVACOES'] = df[['REPROVACOES_MAT_1', 'REPROVACOES_MAT_2', 
                                       'REPROVACOES_MAT_3', 'REPROVACOES_MAT_4']].sum(axis=1)
        
        # Status por matéria (cursou ou não)
        for i in range(1, 5):
            nota_col = f'NOTA_MAT_{i}'
            reprov_col = f'REPROVACOES_MAT_{i}'
            status_col = f'STATUS_MAT_{i}'
            df[status_col] = df.apply(
                lambda row: 'NAO_CURSOU' if pd.isna(row[nota_col]) or row[nota_col] == 0 
                else 'REPROVADO' if row[reprov_col] > 0 
                else 'APROVADO', axis=1
            )
        
        # Adicionar timestamp de transformação
        df['data_transformacao'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        
        # Salvar na camada Silver
        os.makedirs(os.path.dirname(output_path), exist_ok=True)
        df.to_csv(output_path, index=False)
        
        return f"Transformação concluída: {len(df)} registros salvos em {output_path}"
    
    transform_data()

silver_pipeline()
