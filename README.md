# Pipeline de Dados com Governança - Data Catalog

## 📋 Visão Geral

Este projeto implementa um pipeline de dados completo baseado na arquitetura Lakehouse (Bronze, Silver, Gold) com foco em governança de dados, utilizando Apache Airflow para orquestração e OpenMetadata para catálogo, lineage e discovery.

## 🏗️ Arquitetura

### Camadas do Lakehouse

```
┌─────────────────────────────────────────────────────────────┐
│                         BRONZE                              │
│  Dados Brutos/Normalizados (curso.txt → alunos_raw.csv)     │
└─────────────────────────────────────────────────────────────┘
                            ↓
┌─────────────────────────────────────────────────────────────┐
│                         SILVER                              │
│  Dados Transformados e Limpos (alunos_transformado.csv)     │
│  - Tratamento de valores ausentes                           │
│  - Padronização de campos                                   │
│  - Validação de consistência                                │
│  - Enriquecimento com colunas derivadas                     │
└─────────────────────────────────────────────────────────────┘
                            ↓
┌─────────────────────────────────────────────────────────────┐
│                          GOLD                               │
│  Dados Agregados e Curados para Consumo                     │
│  - kpis_dashboard.csv                                       │
│  - analise_risco.csv                                        │
│  - analise_engajamento.csv                                  │
│  - insights.csv                                             │
└─────────────────────────────────────────────────────────────┘
```

### Componentes

- **Apache Airflow**: Orquestração de pipelines
- **OpenMetadata**: Catálogo de dados, lineage e discovery
- **PostgreSQL**: Banco de dados para Airflow e OpenMetadata
- **Elasticsearch**: Indexação de metadados
- **Jupyter Notebook**: Análise exploratória e ML

## 🚀 Instalação e Configuração

### Pré-requisitos

- Docker e Docker Compose instalados
- Mínimo 8GB de RAM disponível


## 🔍 Governança de Dados

### 1. Catálogo de Dados (Data Catalog)

O OpenMetadata automaticamente cataloga:
- **Pipelines**: Todas as DAGs do Airflow com descrições e proprietários
- **Datasets**: Arquivos CSV das camadas Bronze, Silver e Gold
- **Schemas**: Estrutura de colunas, tipos de dados e nullability

**Como verificar:**
1. Acesse OpenMetadata: http://localhost:8585
2. Navegue até "Pipelines" para ver as DAGs
3. Navegue até "Tables" para ver os datasets

### 2. Lineage (Rastreamento de Dados)

O lineage mostra o fluxo de dados entre as camadas:

```
curso.txt → Bronze (alunos_raw.csv) → Silver (alunos_transformado.csv) → Gold (kpis, análises, insights)
```

**Como verificar:**
1. No OpenMetadata, clique em qualquer pipeline
2. Visualize o grafo de lineage mostrando inputs e outputs
3. Rastreie a origem dos dados até o destino final

### 3. Discovery (Descoberta de Dados)

Metadados enriquecidos facilitam a descoberta:
- Descrições das DAGs e tasks
- Proprietários dos pipelines
- Documentação inline no código
- Tags de classificação

**Como verificar:**
1. Use a busca do OpenMetadata para encontrar datasets
2. Visualize descrições e documentação
3. Filtre por tags e proprietários

### 4. Qualidade de Dados (Data Quality)

Cada camada possui DAG de teste que valida:

**Bronze:**
- Dataset não vazio
- Colunas obrigatórias presentes
- Valores ausentes identificados

**Silver:**
- Sem valores ausentes em campos críticos
- Range de notas válido (0-10)
- Colunas derivadas criadas

**Gold:**
- Todos os arquivos de saída gerados
- Métricas calculadas corretamente

## 📈 Dados Gerados

### Camada Bronze
- `alunos_raw.csv`: Dados brutos com metadados de ingestão

### Camada Silver
- `alunos_transformado.csv`: Dados limpos e enriquecidos com:
  - Valores ausentes tratados
  - Campos padronizados
  - Colunas derivadas: MEDIA_GERAL, TAXA_PRESENCA, INDICE_ENGAJAMENTO, TOTAL_REPROVACOES, STATUS_MAT_*

### Camada Gold

**kpis_dashboard.csv:**
- Taxa de alunos por perfil
- Taxa de reprovação por matéria
- Média geral da turma
- Taxa de absenteísmo

**analise_risco.csv:**
- Alunos em situação crítica
- Correlação faltas vs desempenho
- Matérias com maior reprovação
- Alunos com múltiplas reprovações

**analise_engajamento.csv:**
- Correlação presença vs desempenho
- Média de tarefas por perfil
- Impacto do inglês no desempenho

**insights.csv:**
- Matérias que precisam de reforço
- Alunos que necessitam tutoria
- Comparativo alunos com/sem inglês


## 📝 Transformações Implementadas

### Camada Silver - Preparação dos Dados

1. **Tratamento de Valores Ausentes:**
   - INGLES: preenchido com 0 (não tem inglês)
   - NOTAS: preenchidas com mediana da matéria

2. **Validação de Consistência:**
   - Alunos com reprovações têm nota ajustada para 0

3. **Padronização:**
   - INGLES: convertido para booleano (0/1)

4. **Enriquecimento:**
   - MEDIA_GERAL: média das 4 matérias
   - TAXA_PRESENCA: percentual de presença
   - INDICE_ENGAJAMENTO: combinação de presença e tarefas
   - TOTAL_REPROVACOES: soma de reprovações
   - STATUS_MAT_*: status por matéria (APROVADO/REPROVADO/NAO_CURSOU)

### Camada Gold - Informações Gerenciais

1. **KPIs Principais:**
   - Distribuição de alunos por perfil
   - Taxas de reprovação
   - Médias de desempenho
   - Indicadores de absenteísmo

2. **Análises de Risco:**
   - Identificação de alunos críticos
   - Correlações entre variáveis
   - Alertas prioritários

3. **Análises de Engajamento:**
   - Impacto da presença
   - Efetividade de tarefas online
   - Benefícios do conhecimento de inglês

4. **Insights Acionáveis:**
   - Priorização de matérias para reforço
   - Identificação de alunos para tutoria
   - Comparativos e tendências

## 🔐 Segurança e Boas Práticas

- Credenciais padrão devem ser alteradas em produção
- Volumes persistentes para dados e metadados
- Healthchecks configurados para todos os serviços
- Logs centralizados no Airflow
- Validação de qualidade em cada camada

## 📚 Referências

- [Apache Airflow Documentation](https://airflow.apache.org/docs/)
- [OpenMetadata Documentation](https://docs.open-metadata.org/)
- [Lakehouse Architecture](https://www.databricks.com/glossary/data-lakehouse)

## 📄 Licença

Este projeto é parte de um trabalho acadêmico de DataOps.

---

**Nota:** Este é um ambiente de desenvolvimento/estudo. Para produção, considere:
- Usar secrets management (AWS Secrets Manager, Vault)
- Implementar autenticação robusta
- Configurar backups automáticos
- Monitoramento e alertas
- Alta disponibilidade e escalabilidade
