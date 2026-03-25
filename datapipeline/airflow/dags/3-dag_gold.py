"""
DAG GOLD - Agregações e Métricas de Negócio
Esta DAG consome o FATO correlacionado da SILVER para criar os ativos curados 
e agregados da GOLD, otimizados para consumo (BI/Dashboard e ML).

INPUTS: /dados/silver/alunos_transformado.csv
OUTPUT: /dados/gold/kpis_dashboard.csv, /dados/gold/analise_risco.csv, 
        /dados/gold/analise_engajamento.csv, /dados/gold/insights.csv
"""
from airflow.sdk import dag, task
from datetime import datetime
import pandas as pd
import os

@dag(
    dag_id='5_gold_aggregation',
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=['gold', 'aggregation', 'analytics'],
    description='Agregações e métricas de negócio na camada Gold',
    owner_links={"owner": "mailto:engenharia.dados@empresa.com"}
)
def gold_pipeline():
    
    @task(task_id='generate_kpis')
    def generate_kpis():
        """Gera KPIs principais para dashboard"""
        print("=== GERANDO KPIs DASHBOARD ===")
        input_path = '/opt/nb/silver/alunos_transformado.csv'
        output_path = '/opt/nb/gold/kpis_dashboard.csv'
        
        try:
            print(f"Lendo arquivo: {input_path}")
            df = pd.read_csv(input_path)
            print(f"✅ Arquivo lido: {len(df)} registros")
            
            kpis = []
            
            # Taxa de alunos por perfil
            print("Calculando distribuição por perfil...")
            perfil_dist = df['PERFIL'].value_counts()
            perfil_pct = round(perfil_dist / len(df) * 100, 2)
            for perfil, count in perfil_dist.items():
                kpis.append({
                    'metrica': f'Alunos_{perfil}',
                    'valor': count,
                    'percentual': perfil_pct[perfil],
                    'categoria': 'Distribuição por Perfil'
                })
            print(f"✅ {len(perfil_dist)} perfis processados")
            
            # Taxa de reprovação geral e por matéria
            print("Calculando taxas de reprovação...")
            for i in range(1, 5):
                col = f'REPROVACOES_MAT_{i}'
                reprovados = (df[col] > 0).sum()
                taxa = round(reprovados / len(df) * 100, 2)
                kpis.append({
                    'metrica': f'Taxa_Reprovacao_MAT_{i}',
                    'valor': reprovados,
                    'percentual': taxa,
                    'categoria': 'Taxa de Reprovação'
                })
            print("✅ Taxas de reprovação calculadas")
            
            # Média geral de notas da turma
            media_turma = round(df['MEDIA_GERAL'].mean(), 2)
            kpis.append({
                'metrica': 'Media_Geral_Turma',
                'valor': media_turma,
                'percentual': None,
                'categoria': 'Desempenho Acadêmico'
            })
            print(f"✅ Média geral da turma: {media_turma}")
            
            # Taxa de absenteísmo
            faltas_media = round(df['FALTAS'].mean(), 2)
            kpis.append({
                'metrica': 'Faltas_Media',
                'valor': faltas_media,
                'percentual': None,
                'categoria': 'Absenteísmo'
            })
            print(f"✅ Média de faltas: {faltas_media}")
            
            kpis_df = pd.DataFrame(kpis)
            os.makedirs(os.path.dirname(output_path), exist_ok=True)
            kpis_df.to_csv(output_path, index=False)
            print(f"✅ Arquivo salvo: {output_path}")
            
            msg = f"✅ KPIs gerados: {len(kpis)} métricas"
            print(msg)
            return msg
            
        except Exception as e:
            error_msg = f"❌ ERRO GERANDO KPIs: {str(e)}"
            print(error_msg)
            raise Exception(error_msg)
    
    @task(task_id='generate_risk_analysis')
    def generate_risk_analysis():
        """Gera análise de risco dos alunos"""
        print("=== GERANDO ANÁLISE DE RISCO ===")
        input_path = '/opt/nb/silver/alunos_transformado.csv'
        output_path = '/opt/nb/gold/analise_risco.csv'
        
        try:
            print(f"Lendo arquivo: {input_path}")
            df = pd.read_csv(input_path)
            print(f"✅ Arquivo lido: {len(df)} registros")
            
            risk_data = []
            
            # Alunos em situação crítica
            print("Identificando alunos críticos...")
            criticos = df[df['PERFIL'] == 'DIFICULDADE']
            risk_data.append({
                'analise': 'Alunos_Criticos',
                'quantidade': len(criticos),
                'percentual': round(len(criticos) / len(df) * 100, 2),
                'detalhes': f"Perfil DIFICULDADE"
            })
            print(f"✅ {len(criticos)} alunos críticos identificados")
            
            # Correlação entre faltas e desempenho
            print("Calculando correlações...")
            corr_faltas = round(df[['FALTAS', 'MEDIA_GERAL']].corr().iloc[0, 1], 3)
            risk_data.append({
                'analise': 'Correlacao_Faltas_Desempenho',
                'quantidade': None,
                'percentual': None,
                'detalhes': f"Correlação: {corr_faltas}"
            })
            print(f"✅ Correlação faltas/desempenho: {corr_faltas}")
            
            # Matérias com maior índice de reprovação
            print("Analisando reprovações por matéria...")
            for i in range(1, 5):
                col = f'REPROVACOES_MAT_{i}'
                reprovados = (df[col] > 0).sum()
                risk_data.append({
                    'analise': f'Reprovacoes_MAT_{i}',
                    'quantidade': reprovados,
                    'percentual': round(reprovados / len(df) * 100, 2),
                    'detalhes': f"Matéria {i}"
                })
            print("✅ Análise de reprovações concluída")
            
            # Alunos com múltiplas reprovações
            multiplas = df[df['TOTAL_REPROVACOES'] >= 3]
            risk_data.append({
                'analise': 'Multiplas_Reprovacoes',
                'quantidade': len(multiplas),
                'percentual': round(len(multiplas) / len(df) * 100, 2),
                'detalhes': "3 ou mais reprovações"
            })
            print(f"✅ {len(multiplas)} alunos com múltiplas reprovações")
            
            risk_df = pd.DataFrame(risk_data)
            os.makedirs(os.path.dirname(output_path), exist_ok=True)
            risk_df.to_csv(output_path, index=False)
            print(f"✅ Arquivo salvo: {output_path}")
            
            msg = f"✅ Análise de risco gerada: {len(risk_data)} indicadores"
            print(msg)
            return msg
            
        except Exception as e:
            error_msg = f"❌ ERRO ANÁLISE DE RISCO: {str(e)}"
            print(error_msg)
            raise Exception(error_msg)
    
    @task(task_id='generate_engagement_analysis')
    def generate_engagement_analysis():
        """Gera análise de engajamento dos alunos"""
        print("=== GERANDO ANÁLISE DE ENGAJAMENTO ===")
        input_path = '/opt/nb/silver/alunos_transformado.csv'
        output_path = '/opt/nb/gold/analise_engajamento.csv'
        
        try:
            print(f"Lendo arquivo: {input_path}")
            df = pd.read_csv(input_path)
            print(f"✅ Arquivo lido: {len(df)} registros")
            
            engagement_data = []
            
            # Relação entre H_AULA_PRES e desempenho
            print("Calculando correlação presença/desempenho...")
            corr_presenca = round(df[['H_AULA_PRES', 'MEDIA_GERAL']].corr().iloc[0, 1], 3)
            engagement_data.append({
                'analise': 'Correlacao_Presenca_Desempenho',
                'valor': corr_presenca,
                'categoria': 'Presença'
            })
            print(f"✅ Correlação: {corr_presenca}")
            
            # Taxa de conclusão de TAREFAS_ONLINE vs perfil
            print("Analisando tarefas online por perfil...")
            for perfil in df['PERFIL'].unique():
                media_tarefas = round(df[df['PERFIL'] == perfil]['TAREFAS_ONLINE'].mean(), 2)
                engagement_data.append({
                    'analise': f'Media_Tarefas_{perfil}',
                    'valor': media_tarefas,
                    'categoria': 'Tarefas Online'
                })
                print(f"  - {perfil}: {media_tarefas} tarefas em média")
            
            # Impacto do conhecimento de INGLES no desempenho
            print("Analisando impacto do inglês...")
            com_ingles = round(df[df['INGLES'] == 1]['MEDIA_GERAL'].mean(), 2)
            sem_ingles = round(df[df['INGLES'] == 0]['MEDIA_GERAL'].mean(), 2)
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
            print(f"✅ Com inglês: {com_ingles} | Sem inglês: {sem_ingles}")
            
            engagement_df = pd.DataFrame(engagement_data)
            os.makedirs(os.path.dirname(output_path), exist_ok=True)
            engagement_df.to_csv(output_path, index=False)
            print(f"✅ Arquivo salvo: {output_path}")
            
            msg = f"✅ Análise de engajamento gerada: {len(engagement_data)} indicadores"
            print(msg)
            return msg
            
        except Exception as e:
            error_msg = f"❌ ERRO ANÁLISE DE ENGAJAMENTO: {str(e)}"
            print(error_msg)
            raise Exception(error_msg)
    
    @task(task_id='generate_insights')
    def generate_insights():
        """Gera insights para melhorias"""
        print("=== GERANDO INSIGHTS ===")
        input_path = '/opt/nb/silver/alunos_transformado.csv'
        output_path = '/opt/nb/gold/insights.csv'
        
        try:
            print(f"Lendo arquivo: {input_path}")
            df = pd.read_csv(input_path)
            print(f"✅ Arquivo lido: {len(df)} registros")
            
            insights = []
            
            # Ranking de matérias que precisam de reforço
            print("Analisando necessidade de reforço por matéria...")
            for i in range(1, 5):
                nota_col = f'NOTA_MAT_{i}'
                media = round(df[nota_col].mean(), 2)
                reprov_col = f'REPROVACOES_MAT_{i}'
                reprovados = (df[reprov_col] > 0).sum()
                prioridade = 'ALTA' if media < 5.5 or reprovados > 10 else 'MEDIA' if media < 6.5 else 'BAIXA'
                
                insights.append({
                    'insight': f'Materia_{i}_Necessita_Reforco',
                    'prioridade': prioridade,
                    'valor': media,
                    'detalhes': f"Média: {media}, Reprovados: {reprovados}"
                })
                print(f"  - Matéria {i}: Prioridade {prioridade} (média: {media})")
            
            # Perfil de alunos que se beneficiariam de tutoria
            print("Identificando alunos que necessitam tutoria...")
            tutoria = df[(df['MEDIA_GERAL'] < 6) & (df['TOTAL_REPROVACOES'] > 0)]
            insights.append({
                'insight': 'Alunos_Necessitam_Tutoria',
                'prioridade': 'ALTA',
                'valor': len(tutoria),
                'detalhes': f"{len(tutoria)} alunos com média < 6 e reprovações"
            })
            print(f"✅ {len(tutoria)} alunos necessitam tutoria")
            
            # Comparativo: alunos com inglês vs sem inglês
            print("Analisando impacto do inglês...")
            com_ingles = round(df[df['INGLES'] == 1]['MEDIA_GERAL'].mean(), 2)
            sem_ingles = round(df[df['INGLES'] == 0]['MEDIA_GERAL'].mean(), 2)
            diferenca = round(com_ingles - sem_ingles, 2)
            
            insights.append({
                'insight': 'Impacto_Ingles_Desempenho',
                'prioridade': 'MEDIA' if abs(diferenca) > 0.5 else 'BAIXA',
                'valor': diferenca,
                'detalhes': f"Diferença de {diferenca} pontos na média"
            })
            print(f"✅ Diferença de desempenho: {diferenca} pontos")
            
            insights_df = pd.DataFrame(insights)
            os.makedirs(os.path.dirname(output_path), exist_ok=True)
            insights_df.to_csv(output_path, index=False)
            print(f"✅ Arquivo salvo: {output_path}")
            
            msg = f"✅ Insights gerados: {len(insights)} recomendações"
            print(msg)
            return msg
            
        except Exception as e:
            error_msg = f"❌ ERRO GERANDO INSIGHTS: {str(e)}"
            print(error_msg)
            raise Exception(error_msg)
    
    # Definir dependências
    kpis = generate_kpis()
    risk = generate_risk_analysis()
    engagement = generate_engagement_analysis()
    insights = generate_insights()

gold_pipeline()
