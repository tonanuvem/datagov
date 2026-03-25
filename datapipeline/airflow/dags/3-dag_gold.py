"""
DAG GOLD - Agregações e Métricas de Negócio
Esta DAG consome o FATO correlacionado da SILVER para criar os ativos curados 
e agregados da GOLD, otimizados para consumo (BI/Dashboard e ML).

INPUTS: /ml/data/silver/alunos_transformado.csv
OUTPUT: /ml/data/gold/kpis_dashboard.csv, /ml/data/gold/analise_risco.csv, 
        /ml/data/gold/analise_engajamento.csv, /ml/data/gold/insights.csv
"""
from airflow.decorators import dag, task
from datetime import datetime
import pandas as pd
import os

@dag(
    dag_id='gold_aggregation',
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=['gold', 'aggregation', 'analytics'],
    description='Agregações e métricas de negócio na camada Gold',
    owner_links={"owner": "mailto:engenharia.dados@empresa.com"}
)
def gold_pipeline():
    
    @task(task_id='generate_kpis')
    def generate_kpis():
        """Gera KPIs principais para dashboard"""
        input_path = '/opt/nb/data/silver/alunos_transformado.csv'
        output_path = '/opt/nb/data/gold/kpis_dashboard.csv'
        
        df = pd.read_csv(input_path)
        
        # KPIs Principais
        kpis = []
        
        # Taxa de alunos por perfil
        perfil_dist = df['PERFIL'].value_counts()
        perfil_pct = (perfil_dist / len(df) * 100).round(2)
        for perfil, count in perfil_dist.items():
            kpis.append({
                'metrica': f'Alunos_{perfil}',
                'valor': count,
                'percentual': perfil_pct[perfil],
                'categoria': 'Distribuição por Perfil'
            })
        
        # Taxa de reprovação geral e por matéria
        for i in range(1, 5):
            col = f'REPROVACOES_MAT_{i}'
            reprovados = (df[col] > 0).sum()
            taxa = (reprovados / len(df) * 100).round(2)
            kpis.append({
                'metrica': f'Taxa_Reprovacao_MAT_{i}',
                'valor': reprovados,
                'percentual': taxa,
                'categoria': 'Taxa de Reprovação'
            })
        
        # Média geral de notas da turma
        kpis.append({
            'metrica': 'Media_Geral_Turma',
            'valor': df['MEDIA_GERAL'].mean().round(2),
            'percentual': None,
            'categoria': 'Desempenho Acadêmico'
        })
        
        # Taxa de absenteísmo
        kpis.append({
            'metrica': 'Faltas_Media',
            'valor': df['FALTAS'].mean().round(2),
            'percentual': None,
            'categoria': 'Absenteísmo'
        })
        
        kpis_df = pd.DataFrame(kpis)
        os.makedirs(os.path.dirname(output_path), exist_ok=True)
        kpis_df.to_csv(output_path, index=False)
        
        return f"KPIs gerados: {len(kpis)} métricas"
    
    @task(task_id='generate_risk_analysis')
    def generate_risk_analysis():
        """Gera análise de risco dos alunos"""
        input_path = '/opt/nb/data/silver/alunos_transformado.csv'
        output_path = '/opt/nb/data/gold/analise_risco.csv'
        
        df = pd.read_csv(input_path)
        
        risk_data = []
        
        # Alunos em situação crítica
        criticos = df[df['PERFIL'] == 'DIFICULDADE']
        risk_data.append({
            'analise': 'Alunos_Criticos',
            'quantidade': len(criticos),
            'percentual': (len(criticos) / len(df) * 100).round(2),
            'detalhes': f"Perfil DIFICULDADE"
        })
        
        # Correlação entre faltas e desempenho
        corr_faltas = df[['FALTAS', 'MEDIA_GERAL']].corr().iloc[0, 1].round(3)
        risk_data.append({
            'analise': 'Correlacao_Faltas_Desempenho',
            'quantidade': None,
            'percentual': None,
            'detalhes': f"Correlação: {corr_faltas}"
        })
        
        # Matérias com maior índice de reprovação
        for i in range(1, 5):
            col = f'REPROVACOES_MAT_{i}'
            reprovados = (df[col] > 0).sum()
            risk_data.append({
                'analise': f'Reprovacoes_MAT_{i}',
                'quantidade': reprovados,
                'percentual': (reprovados / len(df) * 100).round(2),
                'detalhes': f"Matéria {i}"
            })
        
        # Alunos com múltiplas reprovações
        multiplas = df[df['TOTAL_REPROVACOES'] >= 3]
        risk_data.append({
            'analise': 'Multiplas_Reprovacoes',
            'quantidade': len(multiplas),
            'percentual': (len(multiplas) / len(df) * 100).round(2),
            'detalhes': "3 ou mais reprovações"
        })
        
        risk_df = pd.DataFrame(risk_data)
        os.makedirs(os.path.dirname(output_path), exist_ok=True)
        risk_df.to_csv(output_path, index=False)
        
        return f"Análise de risco gerada: {len(risk_data)} indicadores"
    
    @task(task_id='generate_engagement_analysis')
    def generate_engagement_analysis():
        """Gera análise de engajamento dos alunos"""
        input_path = '/opt/nb/data/silver/alunos_transformado.csv'
        output_path = '/opt/nb/data/gold/analise_engajamento.csv'
        
        df = pd.read_csv(input_path)
        
        engagement_data = []
        
        # Relação entre H_AULA_PRES e desempenho
        corr_presenca = df[['H_AULA_PRES', 'MEDIA_GERAL']].corr().iloc[0, 1].round(3)
        engagement_data.append({
            'analise': 'Correlacao_Presenca_Desempenho',
            'valor': corr_presenca,
            'categoria': 'Presença'
        })
        
        # Taxa de conclusão de TAREFAS_ONLINE vs perfil
        for perfil in df['PERFIL'].unique():
            media_tarefas = df[df['PERFIL'] == perfil]['TAREFAS_ONLINE'].mean().round(2)
            engagement_data.append({
                'analise': f'Media_Tarefas_{perfil}',
                'valor': media_tarefas,
                'categoria': 'Tarefas Online'
            })
        
        # Impacto do conhecimento de INGLES no desempenho
        com_ingles = df[df['INGLES'] == 1]['MEDIA_GERAL'].mean().round(2)
        sem_ingles = df[df['INGLES'] == 0]['MEDIA_GERAL'].mean().round(2)
        engagement_data.append({
            'analise': 'Media_Com_Ingles',
            'valor': com_ingles,
            'categoria': 'Impacto Inglês'
        })
        engagement_data.append({
            'analise': 'Media_Sem_Ingles',
            'valor': sem_ingles,
            'categoria': 'Impacto Inglês'
        })
        
        engagement_df = pd.DataFrame(engagement_data)
        os.makedirs(os.path.dirname(output_path), exist_ok=True)
        engagement_df.to_csv(output_path, index=False)
        
        return f"Análise de engajamento gerada: {len(engagement_data)} indicadores"
    
    @task(task_id='generate_insights')
    def generate_insights():
        """Gera insights para melhorias"""
        input_path = '/opt/nb/data/silver/alunos_transformado.csv'
        output_path = '/opt/nb/data/gold/insights.csv'
        
        df = pd.read_csv(input_path)
        
        insights = []
        
        # Ranking de matérias que precisam de reforço
        for i in range(1, 5):
            nota_col = f'NOTA_MAT_{i}'
            media = df[nota_col].mean().round(2)
            reprov_col = f'REPROVACOES_MAT_{i}'
            reprovados = (df[reprov_col] > 0).sum()
            
            insights.append({
                'insight': f'Materia_{i}_Necessita_Reforco',
                'prioridade': 'ALTA' if media < 5.5 or reprovados > 10 else 'MEDIA' if media < 6.5 else 'BAIXA',
                'valor': media,
                'detalhes': f"Média: {media}, Reprovados: {reprovados}"
            })
        
        # Perfil de alunos que se beneficiariam de tutoria
        tutoria = df[(df['MEDIA_GERAL'] < 6) & (df['TOTAL_REPROVACOES'] > 0)]
        insights.append({
            'insight': 'Alunos_Necessitam_Tutoria',
            'prioridade': 'ALTA',
            'valor': len(tutoria),
            'detalhes': f"{len(tutoria)} alunos com média < 6 e reprovações"
        })
        
        # Comparativo: alunos com inglês vs sem inglês
        com_ingles = df[df['INGLES'] == 1]['MEDIA_GERAL'].mean().round(2)
        sem_ingles = df[df['INGLES'] == 0]['MEDIA_GERAL'].mean().round(2)
        diferenca = (com_ingles - sem_ingles).round(2)
        
        insights.append({
            'insight': 'Impacto_Ingles_Desempenho',
            'prioridade': 'MEDIA' if abs(diferenca) > 0.5 else 'BAIXA',
            'valor': diferenca,
            'detalhes': f"Diferença de {diferenca} pontos na média"
        })
        
        insights_df = pd.DataFrame(insights)
        os.makedirs(os.path.dirname(output_path), exist_ok=True)
        insights_df.to_csv(output_path, index=False)
        
        return f"Insights gerados: {len(insights)} recomendações"
    
    # Definir dependências
    kpis = generate_kpis()
    risk = generate_risk_analysis()
    engagement = generate_engagement_analysis()
    insights = generate_insights()

gold_pipeline()
