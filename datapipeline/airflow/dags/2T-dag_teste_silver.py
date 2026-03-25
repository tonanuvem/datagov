"""
DAG TESTE SILVER - Validação da Camada Silver
Esta DAG testa a qualidade dos dados transformados na camada Silver.

INPUTS: /ml/data/silver/alunos_transformado.csv
OUTPUT: Logs de validação
"""
from airflow.decorators import dag, task
from datetime import datetime
import pandas as pd

@dag(
    dag_id='silver_test',
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=['silver', 'test', 'quality'],
    description='Testes de qualidade da camada Silver',
    owner_links={"owner": "mailto:engenharia.dados@empresa.com"}
)
def silver_test_pipeline():
    
    @task(task_id='validate_silver_data')
    def validate_silver_data():
        """Valida dados transformados da camada Silver"""
        input_path = '/opt/nb/data/silver/alunos_transformado.csv'
        
        df = pd.read_csv(input_path)
        
        # Testes de qualidade
        tests = []
        
        # 1. Verificar se não há valores ausentes em campos críticos
        critical_cols = ['INGLES', 'NOTA_MAT_1', 'NOTA_MAT_2', 'NOTA_MAT_3', 'NOTA_MAT_4']
        for col in critical_cols:
            missing = df[col].isnull().sum()
            tests.append({
                "teste": f"Valores ausentes em {col}",
                "resultado": "PASSOU" if missing == 0 else "FALHOU",
                "detalhes": f"{missing} valores ausentes"
            })
        
        # 2. Verificar range de notas (0-10)
        for col in ['NOTA_MAT_1', 'NOTA_MAT_2', 'NOTA_MAT_3', 'NOTA_MAT_4', 'MEDIA_GERAL']:
            invalid = ((df[col] < 0) | (df[col] > 10)).sum()
            tests.append({
                "teste": f"Range de {col} (0-10)",
                "resultado": "PASSOU" if invalid == 0 else "FALHOU",
                "detalhes": f"{invalid} valores fora do range"
            })
        
        # 3. Verificar se colunas derivadas foram criadas
        derived_cols = ['MEDIA_GERAL', 'TAXA_PRESENCA', 'INDICE_ENGAJAMENTO', 'TOTAL_REPROVACOES']
        for col in derived_cols:
            exists = col in df.columns
            tests.append({
                "teste": f"Coluna {col} criada",
                "resultado": "PASSOU" if exists else "FALHOU",
                "detalhes": "Coluna presente" if exists else "Coluna ausente"
            })
        
        return {
            "total_registros": len(df),
            "testes_executados": len(tests),
            "testes_passaram": sum(1 for t in tests if t['resultado'] == 'PASSOU'),
            "detalhes": tests
        }
    
    validate_silver_data()

silver_test_pipeline()
