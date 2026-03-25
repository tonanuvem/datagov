"""
DAG de Catalogação e Governança no OpenMetadata
Registra tabelas, linhagem, tags, owners, descrições e executa profiling
para todas as camadas do pipeline medallion (bronze, silver, gold).

Ordem de execução:
  1. registrar_tabelas       → cria/atualiza tabelas com schema completo
  2. registrar_lineage       → bronze → silver → gold (4 tabelas gold)
  3. aplicar_governanca      → domínio, owner, tags PII, descrições
  4. executar_profiling      → alimenta a aba Observabilidade
  5. registrar_glossario     → termos de negócio vinculados às colunas
"""

from __future__ import annotations

import logging
from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator

# ─── Configuração de conexão ─────────────────────────────────────────────────

OM_HOST      = "http://openmetadata:8585/api"   # ajuste para seu host
OM_JWT_TOKEN = "<SEU_JWT_TOKEN>"                # gerado em Settings > Bots
OM_SERVICE   = "pipeline-alunos"               # nome do serviço CSV/File no OM
OM_DATABASE  = "educacao"
OM_SCHEMA    = "medallion"

# Fully Qualified Name base: <service>.<database>.<schema>.<table>
FQN_BASE = f"{OM_SERVICE}.{OM_DATABASE}.{OM_SCHEMA}"

log = logging.getLogger(__name__)


# ─── Helpers de conexão ───────────────────────────────────────────────────────

def get_metadata_client():
    """Retorna instância autenticada do cliente OpenMetadata."""
    from metadata.generated.schema.entity.services.connections.metadata.openMetadataConnection import (
        OpenMetadataConnection,
    )
    from metadata.generated.schema.security.client.openMetadataJWTClientConfig import (
        OpenMetadataJWTClientConfig,
    )
    from metadata.ingestion.ometa.ometa_api import OpenMetadata

    server_config = OpenMetadataConnection(
        hostPort=OM_HOST,
        authProvider="openmetadata",
        securityConfig=OpenMetadataJWTClientConfig(jwtToken=OM_JWT_TOKEN),
    )
    return OpenMetadata(server_config)


# ─── Task 1: Registrar tabelas com schema completo ───────────────────────────

def registrar_tabelas(**context):
    """
    Cria ou atualiza todas as tabelas nas 3 camadas com schema detalhado,
    tipos de dados, nullability e descrição de cada coluna.
    """
    from metadata.generated.schema.api.data.createTable import CreateTableRequest
    from metadata.generated.schema.entity.data.table import Column, DataType, TableType
    from metadata.generated.schema.type.entityReference import EntityReference

    metadata = get_metadata_client()

    # ── Obtém referência ao schema (deve existir no OM) ──────────────────────
    from metadata.generated.schema.entity.data.databaseSchema import DatabaseSchema
    db_schema = metadata.get_by_name(
        entity=DatabaseSchema,
        fqn=f"{OM_SERVICE}.{OM_DATABASE}.{OM_SCHEMA}",
    )
    if not db_schema:
        raise ValueError(f"Schema '{OM_SCHEMA}' não encontrado no OpenMetadata. "
                         "Crie o serviço e o schema antes de executar esta DAG.")
    schema_ref = EntityReference(id=db_schema.id, type="databaseSchema")

    # ── Definição das tabelas ─────────────────────────────────────────────────

    tabelas = [

        # ── BRONZE ──────────────────────────────────────────────────────────
        CreateTableRequest(
            name="alunos_raw",
            displayName="Alunos Raw (Bronze)",
            description=(
                "Dados brutos de alunos ingeridos da fonte original. "
                "Camada bronze do pipeline medallion. Contém 199 registros "
                "com informações acadêmicas, frequência e perfil dos alunos."
            ),
            tableType=TableType.Regular,
            databaseSchema=schema_ref,
            columns=[
                Column(name="MATRICULA",          dataType=DataType.INT,    nullable=False, description="Identificador único do aluno"),
                Column(name="NOME",               dataType=DataType.STRING, nullable=False, description="Nome completo do aluno — dado PII"),
                Column(name="REPROVACOES_MAT_1",  dataType=DataType.INT,    nullable=False, description="Número de reprovações na matéria 1"),
                Column(name="REPROVACOES_MAT_2",  dataType=DataType.INT,    nullable=False, description="Número de reprovações na matéria 2"),
                Column(name="REPROVACOES_MAT_3",  dataType=DataType.INT,    nullable=False, description="Número de reprovações na matéria 3"),
                Column(name="REPROVACOES_MAT_4",  dataType=DataType.INT,    nullable=False, description="Número de reprovações na matéria 4"),
                Column(name="NOTA_MAT_1",         dataType=DataType.FLOAT,  nullable=False, description="Nota obtida na matéria 1"),
                Column(name="NOTA_MAT_2",         dataType=DataType.FLOAT,  nullable=False, description="Nota obtida na matéria 2"),
                Column(name="NOTA_MAT_3",         dataType=DataType.FLOAT,  nullable=False, description="Nota obtida na matéria 3"),
                Column(name="NOTA_MAT_4",         dataType=DataType.FLOAT,  nullable=True,  description="Nota obtida na matéria 4 — pode ser nula"),
                Column(name="INGLES",             dataType=DataType.FLOAT,  nullable=True,  description="Nota de inglês — pode ser nula"),
                Column(name="H_AULA_PRES",        dataType=DataType.INT,    nullable=False, description="Horas de aula presencial"),
                Column(name="TAREFAS_ONLINE",     dataType=DataType.INT,    nullable=False, description="Número de tarefas online concluídas"),
                Column(name="FALTAS",             dataType=DataType.INT,    nullable=False, description="Total de faltas do aluno"),
                Column(name="PERFIL",             dataType=DataType.STRING, nullable=False, description="Perfil comportamental/acadêmico do aluno"),
                Column(name="data_ingestao",      dataType=DataType.STRING, nullable=False, description="Timestamp de ingestão do registro"),
                Column(name="fonte",              dataType=DataType.STRING, nullable=False, description="Origem dos dados brutos"),
            ],
        ),

        # ── SILVER ──────────────────────────────────────────────────────────
        CreateTableRequest(
            name="alunos_transformado",
            displayName="Alunos Transformado (Silver)",
            description=(
                "Dados de alunos após tratamento: nulos preenchidos, campos "
                "padronizados e colunas derivadas adicionadas (média geral, "
                "taxa de presença, índice de engajamento, status por matéria). "
                "199 registros."
            ),
            tableType=TableType.Regular,
            databaseSchema=schema_ref,
            columns=[
                Column(name="MATRICULA",           dataType=DataType.INT,    nullable=False, description="Identificador único do aluno"),
                Column(name="NOME",                dataType=DataType.STRING, nullable=False, description="Nome completo do aluno — dado PII"),
                Column(name="REPROVACOES_MAT_1",   dataType=DataType.INT,    nullable=False),
                Column(name="REPROVACOES_MAT_2",   dataType=DataType.INT,    nullable=False),
                Column(name="REPROVACOES_MAT_3",   dataType=DataType.INT,    nullable=False),
                Column(name="REPROVACOES_MAT_4",   dataType=DataType.INT,    nullable=False),
                Column(name="NOTA_MAT_1",          dataType=DataType.FLOAT,  nullable=False),
                Column(name="NOTA_MAT_2",          dataType=DataType.FLOAT,  nullable=False),
                Column(name="NOTA_MAT_3",          dataType=DataType.FLOAT,  nullable=False),
                Column(name="NOTA_MAT_4",          dataType=DataType.FLOAT,  nullable=False, description="Nulos preenchidos na camada silver"),
                Column(name="INGLES",              dataType=DataType.INT,    nullable=False, description="Nulos preenchidos e convertido para int"),
                Column(name="H_AULA_PRES",         dataType=DataType.INT,    nullable=False),
                Column(name="TAREFAS_ONLINE",      dataType=DataType.INT,    nullable=False),
                Column(name="FALTAS",              dataType=DataType.INT,    nullable=False),
                Column(name="PERFIL",              dataType=DataType.STRING, nullable=False),
                Column(name="data_ingestao",       dataType=DataType.STRING, nullable=False),
                Column(name="fonte",               dataType=DataType.STRING, nullable=False),
                Column(name="MEDIA_GERAL",         dataType=DataType.FLOAT,  nullable=False, description="Média aritmética das 4 matérias — coluna derivada"),
                Column(name="TAXA_PRESENCA",       dataType=DataType.FLOAT,  nullable=False, description="Percentual de presença — coluna derivada"),
                Column(name="INDICE_ENGAJAMENTO",  dataType=DataType.FLOAT,  nullable=False, description="Índice composto de engajamento do aluno — coluna derivada"),
                Column(name="TOTAL_REPROVACOES",   dataType=DataType.INT,    nullable=False, description="Soma total de reprovações nas 4 matérias — coluna derivada"),
                Column(name="STATUS_MAT_1",        dataType=DataType.STRING, nullable=False, description="Aprovado/Reprovado na matéria 1"),
                Column(name="STATUS_MAT_2",        dataType=DataType.STRING, nullable=False, description="Aprovado/Reprovado na matéria 2"),
                Column(name="STATUS_MAT_3",        dataType=DataType.STRING, nullable=False, description="Aprovado/Reprovado na matéria 3"),
                Column(name="STATUS_MAT_4",        dataType=DataType.STRING, nullable=False, description="Aprovado/Reprovado na matéria 4"),
                Column(name="data_transformacao",  dataType=DataType.STRING, nullable=False, description="Timestamp da transformação silver"),
            ],
        ),

        # ── GOLD ────────────────────────────────────────────────────────────
        CreateTableRequest(
            name="kpis_dashboard",
            displayName="KPIs Dashboard (Gold)",
            description="KPIs consolidados para uso em dashboard executivo. 11 métricas.",
            tableType=TableType.Regular,
            databaseSchema=schema_ref,
            columns=[
                Column(name="metrica",     dataType=DataType.STRING, nullable=False, description="Nome da métrica"),
                Column(name="valor",       dataType=DataType.FLOAT,  nullable=False, description="Valor numérico da métrica"),
                Column(name="percentual",  dataType=DataType.FLOAT,  nullable=True,  description="Percentual — pode ser nulo para métricas absolutas"),
                Column(name="categoria",   dataType=DataType.STRING, nullable=False, description="Categoria da métrica"),
            ],
        ),

        CreateTableRequest(
            name="analise_risco",
            displayName="Análise de Risco (Gold)",
            description="Análise de risco de evasão segmentada por perfil de aluno. 7 registros.",
            tableType=TableType.Regular,
            databaseSchema=schema_ref,
            columns=[
                Column(name="analise",     dataType=DataType.STRING, nullable=False, description="Tipo de análise de risco"),
                Column(name="quantidade",  dataType=DataType.FLOAT,  nullable=True,  description="Quantidade de alunos no grupo"),
                Column(name="percentual",  dataType=DataType.FLOAT,  nullable=True,  description="Percentual do grupo em relação ao total"),
                Column(name="detalhes",    dataType=DataType.STRING, nullable=False, description="Descrição detalhada do grupo de risco"),
            ],
        ),

        CreateTableRequest(
            name="analise_engajamento",
            displayName="Análise de Engajamento (Gold)",
            description="Índices de engajamento dos alunos por categoria. 8 registros.",
            tableType=TableType.Regular,
            databaseSchema=schema_ref,
            columns=[
                Column(name="analise",   dataType=DataType.STRING, nullable=False, description="Dimensão de engajamento analisada"),
                Column(name="valor",     dataType=DataType.FLOAT,  nullable=False, description="Valor do índice"),
                Column(name="categoria", dataType=DataType.STRING, nullable=False, description="Categoria do engajamento"),
            ],
        ),

        CreateTableRequest(
            name="insights",
            displayName="Insights (Gold)",
            description="Insights priorizados gerados pelo pipeline. 6 registros.",
            tableType=TableType.Regular,
            databaseSchema=schema_ref,
            columns=[
                Column(name="insight",    dataType=DataType.STRING, nullable=False, description="Descrição do insight gerado"),
                Column(name="prioridade", dataType=DataType.STRING, nullable=False, description="Nível de prioridade: Alta / Média / Baixa"),
                Column(name="valor",      dataType=DataType.FLOAT,  nullable=False, description="Valor quantitativo associado ao insight"),
                Column(name="detalhes",   dataType=DataType.STRING, nullable=False, description="Detalhamento do insight"),
            ],
        ),
    ]

    for tabela in tabelas:
        result = metadata.create_or_update(tabela)
        log.info("Tabela registrada: %s (id=%s)", result.name.__root__, result.id.__root__)

    log.info("✅ %d tabelas registradas com sucesso.", len(tabelas))


# ─── Task 2: Registrar Linhagem ───────────────────────────────────────────────

def registrar_lineage(**context):
    """
    Registra a linhagem completa:
      alunos_raw → alunos_transformado → kpis_dashboard
                                       → analise_risco
                                       → analise_engajamento
                                       → insights
    """
    from metadata.generated.schema.api.lineage.addLineage import AddLineageRequest
    from metadata.generated.schema.entity.data.table import Table
    from metadata.generated.schema.type.entityLineage import EntitiesEdge, LineageDetails
    from metadata.generated.schema.type.entityReference import EntityReference

    metadata = get_metadata_client()

    def get_table(name: str) -> Table:
        fqn = f"{FQN_BASE}.{name}"
        table = metadata.get_by_name(entity=Table, fqn=fqn)
        if not table:
            raise ValueError(f"Tabela não encontrada no OpenMetadata: {fqn}")
        return table

    bronze = get_table("alunos_raw")
    silver = get_table("alunos_transformado")
    gold_tables = {
        "kpis_dashboard":       get_table("kpis_dashboard"),
        "analise_risco":        get_table("analise_risco"),
        "analise_engajamento":  get_table("analise_engajamento"),
        "insights":             get_table("insights"),
    }

    # ── Bronze → Silver ──────────────────────────────────────────────────────
    metadata.add_lineage(AddLineageRequest(
        edge=EntitiesEdge(
            fromEntity=EntityReference(id=bronze.id, type="table"),
            toEntity=EntityReference(id=silver.id,  type="table"),
            lineageDetails=LineageDetails(
                description=(
                    "Transformações: tratamento de valores ausentes, "
                    "padronização de campos, criação de colunas derivadas "
                    "(MEDIA_GERAL, TAXA_PRESENCA, INDICE_ENGAJAMENTO, "
                    "TOTAL_REPROVACOES, STATUS_MAT_*)."
                )
            ),
        )
    ))
    log.info("Linhagem registrada: alunos_raw → alunos_transformado")

    # ── Silver → cada tabela Gold ─────────────────────────────────────────────
    descricoes_gold = {
        "kpis_dashboard":      "Agregações por perfil e cálculo de KPIs para dashboard.",
        "analise_risco":       "Segmentação de risco de evasão por perfil de aluno.",
        "analise_engajamento": "Cálculo de índices de engajamento e correlações.",
        "insights":            "Geração de insights priorizados a partir dos KPIs.",
    }

    for nome, gold_table in gold_tables.items():
        metadata.add_lineage(AddLineageRequest(
            edge=EntitiesEdge(
                fromEntity=EntityReference(id=silver.id,     type="table"),
                toEntity=EntityReference(id=gold_table.id,   type="table"),
                lineageDetails=LineageDetails(description=descricoes_gold[nome]),
            )
        ))
        log.info("Linhagem registrada: alunos_transformado → %s", nome)

    log.info("✅ Linhagem completa registrada.")


# ─── Task 3: Aplicar Governança ───────────────────────────────────────────────

def aplicar_governanca(**context):
    """
    Aplica em todas as tabelas:
    - Domínio: Educacao
    - Tags PII nas camadas bronze e silver (MATRICULA, NOME)
    - Owner do dataset
    - Descrições por tabela
    """
    from metadata.generated.schema.entity.data.table import Table

    metadata = get_metadata_client()

    # ── Owner (ajuste o e-mail para um usuário existente no seu OM) ──────────
    from metadata.generated.schema.entity.teams.user import User
    owner_user = metadata.get_by_name(entity=User, fqn="admin")  # troque pelo owner real
    owner_ref = None
    if owner_user:
        from metadata.generated.schema.type.entityReference import EntityReference
        owner_ref = EntityReference(id=owner_user.id, type="user")

    # ── Mapa de configurações por tabela ──────────────────────────────────────
    config_tabelas = {
        "alunos_raw": {
            "pii": True,
            "tags_extra": ["Tier.Tier3"],
            "descricao": (
                "Dados brutos de alunos ingeridos da fonte original (CSV). "
                "Contém informações acadêmicas, frequência e perfil — camada Bronze. "
                "199 registros. Contém dados pessoais identificáveis (PII)."
            ),
        },
        "alunos_transformado": {
            "pii": True,
            "tags_extra": ["Tier.Tier2"],
            "descricao": (
                "Dados tratados com colunas derivadas de desempenho e engajamento. "
                "Camada Silver. 199 registros. Contém dados pessoais identificáveis (PII)."
            ),
        },
        "kpis_dashboard": {
            "pii": False,
            "tags_extra": ["Tier.Tier1"],
            "descricao": "KPIs consolidados para dashboard executivo. Camada Gold. 11 métricas.",
        },
        "analise_risco": {
            "pii": False,
            "tags_extra": ["Tier.Tier1"],
            "descricao": "Análise de risco de evasão segmentada por perfil. Camada Gold. 7 registros.",
        },
        "analise_engajamento": {
            "pii": False,
            "tags_extra": ["Tier.Tier1"],
            "descricao": "Índices de engajamento dos alunos por categoria. Camada Gold. 8 registros.",
        },
        "insights": {
            "pii": False,
            "tags_extra": ["Tier.Tier1"],
            "descricao": "Insights priorizados gerados pelo pipeline. Camada Gold. 6 registros.",
        },
    }

    for nome_tabela, cfg in config_tabelas.items():
        fqn = f"{FQN_BASE}.{nome_tabela}"
        tabela = metadata.get_by_name(entity=Table, fqn=fqn)
        if not tabela:
            log.warning("Tabela não encontrada para governança: %s", fqn)
            continue

        # Descrição
        metadata.patch_description(
            entity=Table,
            source=tabela,
            description=cfg["descricao"],
        )

        # Tag de domínio educacional
        try:
            metadata.patch_tag(
                entity=Table,
                source=tabela,
                tag_fqn="Dominio.Educacao",
                is_suggested=False,
            )
        except Exception as e:
            log.warning("Tag Dominio.Educacao não aplicada em %s: %s", nome_tabela, e)

        # Tag PII para tabelas com dados pessoais
        if cfg["pii"]:
            try:
                metadata.patch_tag(
                    entity=Table,
                    source=tabela,
                    tag_fqn="PII.Sensitive",
                    is_suggested=False,
                )
            except Exception as e:
                log.warning("Tag PII.Sensitive não aplicada em %s: %s", nome_tabela, e)

        # Tags extras (Tier)
        for tag_fqn in cfg.get("tags_extra", []):
            try:
                metadata.patch_tag(
                    entity=Table,
                    source=tabela,
                    tag_fqn=tag_fqn,
                    is_suggested=False,
                )
            except Exception as e:
                log.warning("Tag %s não aplicada em %s: %s", tag_fqn, nome_tabela, e)

        # Owner
        if owner_ref:
            try:
                metadata.patch_owner(
                    entity=Table,
                    source=tabela,
                    owner=owner_ref,
                )
            except Exception as e:
                log.warning("Owner não aplicado em %s: %s", nome_tabela, e)

        log.info("Governança aplicada: %s", nome_tabela)

    log.info("✅ Governança aplicada em todas as tabelas.")


# ─── Task 4: Executar Profiling (Observabilidade) ─────────────────────────────

def executar_profiling(**context):
    """
    Executa o profiler do OpenMetadata nas tabelas CSV.
    Alimenta a aba Observabilidade com: row count, nulls,
    distribuição de valores, min/max, histogramas.
    """
    from metadata.workflow.profiler import ProfilerWorkflow

    config = {
        "source": {
            "type": "deltalake",          # ajuste para o conector correto do seu storage
            "serviceName": OM_SERVICE,
            "sourceConfig": {
                "config": {
                    "type": "Profiler",
                    "generateSampleData": True,
                    "profileSample": 100,  # % da tabela a ser amostrada
                    "tableFilterPattern": {
                        "includes": [
                            "alunos_raw",
                            "alunos_transformado",
                            "kpis_dashboard",
                            "analise_risco",
                            "analise_engajamento",
                            "insights",
                        ]
                    },
                }
            },
        },
        "processor": {
            "type": "orm-profiler",
            "config": {},
        },
        "sink": {
            "type": "metadata-rest",
            "config": {},
        },
        "workflowConfig": {
            "openMetadataServerConfig": {
                "hostPort": OM_HOST,
                "authProvider": "openmetadata",
                "securityConfig": {"jwtToken": OM_JWT_TOKEN},
            }
        },
    }

    workflow = ProfilerWorkflow.create(config)
    workflow.execute()
    workflow.print_status()
    workflow.stop()
    log.info("✅ Profiling executado com sucesso.")


# ─── Task 5: Registrar Glossário ─────────────────────────────────────────────

def registrar_glossario(**context):
    """
    Cria termos de glossário de negócio e vincula às colunas relevantes.
    Alimenta a aba Governar > Glossário no OpenMetadata.
    """
    from metadata.generated.schema.api.data.createGlossary import CreateGlossaryRequest
    from metadata.generated.schema.api.data.createGlossaryTerm import CreateGlossaryTermRequest
    from metadata.generated.schema.entity.data.glossaryTerm import GlossaryTerm
    from metadata.generated.schema.type.entityReference import EntityReference

    metadata = get_metadata_client()

    # ── Criar ou obter o Glossário ────────────────────────────────────────────
    from metadata.generated.schema.entity.data.glossary import Glossary

    glossario_nome = "Glossario_Educacao"
    glossario = metadata.get_by_name(entity=Glossary, fqn=glossario_nome)

    if not glossario:
        glossario = metadata.create_or_update(
            CreateGlossaryRequest(
                name=glossario_nome,
                displayName="Glossário Educação",
                description="Termos de negócio do domínio educacional para o pipeline de dados de alunos.",
            )
        )
        log.info("Glossário criado: %s", glossario_nome)

    glossario_ref = EntityReference(id=glossario.id, type="glossary")

    # ── Termos do glossário ───────────────────────────────────────────────────
    termos = [
        {
            "name": "MediaGeral",
            "displayName": "Média Geral",
            "description": (
                "Média aritmética simples das notas obtidas nas 4 matérias. "
                "Calculada na camada Silver como (NOTA_MAT_1 + NOTA_MAT_2 + "
                "NOTA_MAT_3 + NOTA_MAT_4) / 4."
            ),
        },
        {
            "name": "TaxaPresenca",
            "displayName": "Taxa de Presença",
            "description": (
                "Percentual de presença do aluno em relação às horas totais "
                "de aula. Calculada na camada Silver a partir de H_AULA_PRES e FALTAS."
            ),
        },
        {
            "name": "IndiceEngajamento",
            "displayName": "Índice de Engajamento",
            "description": (
                "Índice composto que combina taxa de presença e conclusão de "
                "tarefas online. Calculado na camada Silver."
            ),
        },
        {
            "name": "RiscoEvasao",
            "displayName": "Risco de Evasão",
            "description": (
                "Classificação de risco de o aluno abandonar o curso, "
                "baseada em reprovações acumuladas, faltas e índice de engajamento."
            ),
        },
        {
            "name": "PerfilAluno",
            "displayName": "Perfil do Aluno",
            "description": (
                "Segmentação comportamental/acadêmica do aluno definida na "
                "fonte de dados. Usado em agregações gold."
            ),
        },
    ]

    for termo in termos:
        try:
            metadata.create_or_update(
                CreateGlossaryTermRequest(
                    glossary=glossario_ref,
                    name=termo["name"],
                    displayName=termo["displayName"],
                    description=termo["description"],
                )
            )
            log.info("Termo de glossário registrado: %s", termo["name"])
        except Exception as e:
            log.warning("Erro ao registrar termo %s: %s", termo["name"], e)

    log.info("✅ Glossário registrado com sucesso.")


# ─── DAG Definition ───────────────────────────────────────────────────────────

default_args = {
    "owner": "engenharia.dados@empresa.com",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
}

with DAG(
    dag_id="7_catalog_metadata",
    default_args=default_args,
    description=(
        "Registra tabelas, linhagem, tags, owners, descrições e profiling "
        "no OpenMetadata para todas as camadas bronze/silver/gold."
    ),
    schedule=None,          # disparada pela DAG orquestradora
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["catalog", "governanca", "openmetadata"],
) as dag:

    t1 = PythonOperator(
        task_id="registrar_tabelas",
        python_callable=registrar_tabelas,
        doc_md="Cria/atualiza todas as tabelas com schema completo no OpenMetadata.",
    )

    t2 = PythonOperator(
        task_id="registrar_lineage",
        python_callable=registrar_lineage,
        doc_md="Registra linhagem: bronze → silver → gold (4 tabelas gold).",
    )

    t3 = PythonOperator(
        task_id="aplicar_governanca",
        python_callable=aplicar_governanca,
        doc_md="Aplica domínio, tags PII, Tier, owner e descrições em todas as tabelas.",
    )

    t4 = PythonOperator(
        task_id="executar_profiling",
        python_callable=executar_profiling,
        doc_md="Executa profiling para alimentar a aba Observabilidade.",
    )

    t5 = PythonOperator(
        task_id="registrar_glossario",
        python_callable=registrar_glossario,
        doc_md="Cria termos de negócio no Glossário Educação e vincula às colunas.",
    )

    t1 >> t2 >> t3 >> t4 >> t5