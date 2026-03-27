"""
DAG de Catalogação e Governança no OpenMetadata
Registra tabelas, linhagem, tags, owners, descrições e popula todas
as abas do OpenMetadata: Linhagem, Observabilidade, Domínios e Governar.

Ordem de execução:
  t0. criar_servico_schema       → cria serviço e schema se não existirem
  t1. registrar_tabelas          → cria/atualiza tabelas com schema completo
                                   (inclui curso_txt como fonte CSV)
  t2. registrar_lineage          → curso_txt → bronze → silver → gold (4 tabelas)
  t3. aplicar_governanca         → owner, tags PII/Tier, descrições
  t4. registrar_glossario        → termos de negócio no Glossário Educação
  t5. vincular_glossario_colunas → vincula termos às colunas da silver
  t6. criar_dominio_produto      → Domínio Educação + Produto de Dados gold
  t7. registrar_metrica_negocio  → Governar > Métricas
  t8. criar_alertas              → Observabilidade > Alertas de qualidade

Grafo de linhagem esperado no OpenMetadata:
  curso_txt (CSV fonte)
      └─► alunos_raw (Bronze)
              └─► alunos_transformado (Silver)
                      ├─► kpis_dashboard      (Gold)
                      ├─► analise_risco       (Gold)
                      ├─► analise_engajamento (Gold)
                      └─► insights            (Gold)

PRÉ-REQUISITO para linhagem via Airflow Lineage Backend:
  As DAGs 1-dag_bronze.py, 2-dag_silver.py e 3-dag_gold.py devem usar
  Asset(uri="openmetadata://pipeline_alunos.educacao.camadas.<tabela>")
  nos parâmetros inlets/outlets de cada task.
"""

from __future__ import annotations

import logging
import os
from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator

# ─── Configuração de conexão ─────────────────────────────────────────────────

OM_HOST      = "http://openmetadata-server:8585/api"
OM_JWT_TOKEN = os.getenv("OPENMETADATA_JWT_TOKEN")
OM_SERVICE   = "pipeline_alunos"
OM_DATABASE  = "educacao"
OM_SCHEMA    = "camadas"

FQN_BASE = f"{OM_SERVICE}.{OM_DATABASE}.{OM_SCHEMA}"

log = logging.getLogger(__name__)


# ─── Helper de conexão ────────────────────────────────────────────────────────

def get_metadata_client():
    from metadata.generated.schema.entity.services.connections.metadata.openMetadataConnection import (
        OpenMetadataConnection,
    )
    from metadata.generated.schema.security.client.openMetadataJWTClientConfig import (
        OpenMetadataJWTClientConfig,
    )
    from metadata.ingestion.ometa.ometa_api import OpenMetadata

    return OpenMetadata(OpenMetadataConnection(
        hostPort=OM_HOST,
        authProvider="openmetadata",
        securityConfig=OpenMetadataJWTClientConfig(jwtToken=OM_JWT_TOKEN),
    ))


# ─── Task 0: Criar serviço, database e schema ────────────────────────────────

def criar_servico_schema(**context):
    """Cria o serviço CustomDatabase, database e schema se não existirem."""
    from metadata.generated.schema.api.services.createDatabaseService import CreateDatabaseServiceRequest
    from metadata.generated.schema.api.data.createDatabase import CreateDatabaseRequest
    from metadata.generated.schema.api.data.createDatabaseSchema import CreateDatabaseSchemaRequest
    from metadata.generated.schema.entity.services.databaseService import (
        DatabaseService, DatabaseServiceType, DatabaseConnection,
    )
    from metadata.generated.schema.entity.services.connections.database.customDatabaseConnection import (
        CustomDatabaseConnection,
    )
    from metadata.generated.schema.entity.data.database import Database
    from metadata.generated.schema.entity.data.databaseSchema import DatabaseSchema

    metadata = get_metadata_client()

    for entity, fqn, create_fn in [
        (
            DatabaseService, OM_SERVICE,
            lambda: metadata.create_or_update(CreateDatabaseServiceRequest(
                name=OM_SERVICE,
                serviceType=DatabaseServiceType.CustomDatabase,
                connection=DatabaseConnection(config=CustomDatabaseConnection(
                    type="CustomDatabase",
                    sourcePythonClass="metadata.ingestion.source.database.customdatabase.metadata.CustomDatabaseSource",
                )),
            ))
        ),
        (
            Database, f"{OM_SERVICE}.{OM_DATABASE}",
            lambda: metadata.create_or_update(CreateDatabaseRequest(
                name=OM_DATABASE, service=OM_SERVICE,
            ))
        ),
        (
            DatabaseSchema, f"{OM_SERVICE}.{OM_DATABASE}.{OM_SCHEMA}",
            lambda: metadata.create_or_update(CreateDatabaseSchemaRequest(
                name=OM_SCHEMA, database=f"{OM_SERVICE}.{OM_DATABASE}",
            ))
        ),
    ]:
        try:
            obj = metadata.get_by_name(entity=entity, fqn=fqn)
            if not obj:
                create_fn()
                log.info("Criado: %s", fqn)
            else:
                log.info("Já existe: %s", fqn)
        except Exception as e:
            log.warning("Erro ao criar %s: %s", fqn, e)

    log.info("✅ Serviço, database e schema verificados/criados.")


# ─── Task 1: Registrar tabelas ────────────────────────────────────────────────

def registrar_tabelas(**context):
    """
    Cria ou atualiza todas as tabelas com schema detalhado.
    Inclui curso_txt como entidade fonte (CSV de origem do pipeline).
    Sem ela catalogada, o Lineage Backend não consegue criar a aresta
    curso_txt → alunos_raw ao processar os inlets/outlets da DAG Bronze.
    """
    from metadata.generated.schema.api.data.createTable import CreateTableRequest
    from metadata.generated.schema.entity.data.table import Column, DataType, TableType
    from metadata.generated.schema.entity.data.databaseSchema import DatabaseSchema

    metadata = get_metadata_client()

    db_schema = metadata.get_by_name(
        entity=DatabaseSchema,
        fqn=f"{OM_SERVICE}.{OM_DATABASE}.{OM_SCHEMA}",
    )
    if not db_schema:
        raise ValueError(
            f"Schema '{OM_SCHEMA}' não encontrado. Execute criar_servico_schema primeiro."
        )

    schema_fqn = f"{OM_SERVICE}.{OM_DATABASE}.{OM_SCHEMA}"

    tabelas = [

        # ── FONTE CSV ────────────────────────────────────────────────────────
        # Registrada como Table para que o Lineage Backend do OpenMetadata
        # consiga resolver o Asset URI:
        #   openmetadata://pipeline_alunos.educacao.camadas.curso_txt
        # declarado como inlet na DAG Bronze (1-dag_bronze.py).
        CreateTableRequest(
            name="curso_txt",
            displayName="Fonte: curso.txt (CSV)",
            description=(
                "Arquivo CSV de origem do pipeline medallion. Contém os dados brutos "
                "dos alunos antes de qualquer processamento. "
                "Localização no servidor: /opt/nb/curso.txt. "
                "É a raiz do grafo de linhagem: curso_txt → Bronze → Silver → Gold."
            ),
            tableType=TableType.Regular,
            databaseSchema=schema_fqn,
            columns=[
                Column(name="MATRICULA",         dataType=DataType.INT,   description="Matrícula do aluno — identificador único na fonte"),
                Column(name="NOME",              dataType=DataType.STRING,description="Nome completo do aluno — dado PII"),
                Column(name="REPROVACOES_MAT_1", dataType=DataType.INT,   description="Reprovações na matéria 1"),
                Column(name="REPROVACOES_MAT_2", dataType=DataType.INT,   description="Reprovações na matéria 2"),
                Column(name="REPROVACOES_MAT_3", dataType=DataType.INT,   description="Reprovações na matéria 3"),
                Column(name="REPROVACOES_MAT_4", dataType=DataType.INT,   description="Reprovações na matéria 4"),
                Column(name="NOTA_MAT_1",        dataType=DataType.FLOAT, description="Nota obtida na matéria 1"),
                Column(name="NOTA_MAT_2",        dataType=DataType.FLOAT, description="Nota obtida na matéria 2"),
                Column(name="NOTA_MAT_3",        dataType=DataType.FLOAT, description="Nota obtida na matéria 3"),
                Column(name="NOTA_MAT_4",        dataType=DataType.FLOAT, description="Nota obtida na matéria 4 — pode ser nula na fonte"),
                Column(name="INGLES",            dataType=DataType.FLOAT, description="Nota de inglês — pode ser nula na fonte"),
                Column(name="H_AULA_PRES",       dataType=DataType.INT,   description="Horas de aula presencial"),
                Column(name="TAREFAS_ONLINE",    dataType=DataType.INT,   description="Tarefas online concluídas"),
                Column(name="FALTAS",            dataType=DataType.INT,   description="Total de faltas"),
                Column(name="PERFIL",            dataType=DataType.STRING,description="Perfil comportamental/acadêmico do aluno"),
            ],
        ),

        # ── BRONZE ──────────────────────────────────────────────────────────
        CreateTableRequest(
            name="alunos_raw",
            displayName="Alunos Raw (Bronze)",
            description=(
                "Dados brutos de alunos ingeridos da fonte original. "
                "Camada bronze do pipeline medallion. 199 registros com "
                "informações acadêmicas, frequência e perfil dos alunos."
            ),
            tableType=TableType.Regular,
            databaseSchema=schema_fqn,
            columns=[
                Column(name="MATRICULA",         dataType=DataType.INT,   description="Identificador único do aluno"),
                Column(name="NOME",              dataType=DataType.STRING,description="Nome completo do aluno — dado PII"),
                Column(name="REPROVACOES_MAT_1", dataType=DataType.INT,   description="Número de reprovações na matéria 1"),
                Column(name="REPROVACOES_MAT_2", dataType=DataType.INT,   description="Número de reprovações na matéria 2"),
                Column(name="REPROVACOES_MAT_3", dataType=DataType.INT,   description="Número de reprovações na matéria 3"),
                Column(name="REPROVACOES_MAT_4", dataType=DataType.INT,   description="Número de reprovações na matéria 4"),
                Column(name="NOTA_MAT_1",        dataType=DataType.FLOAT, description="Nota obtida na matéria 1"),
                Column(name="NOTA_MAT_2",        dataType=DataType.FLOAT, description="Nota obtida na matéria 2"),
                Column(name="NOTA_MAT_3",        dataType=DataType.FLOAT, description="Nota obtida na matéria 3"),
                Column(name="NOTA_MAT_4",        dataType=DataType.FLOAT, description="Nota obtida na matéria 4 — pode ser nula"),
                Column(name="INGLES",            dataType=DataType.FLOAT, description="Nota de inglês — pode ser nula"),
                Column(name="H_AULA_PRES",       dataType=DataType.INT,   description="Horas de aula presencial"),
                Column(name="TAREFAS_ONLINE",    dataType=DataType.INT,   description="Número de tarefas online concluídas"),
                Column(name="FALTAS",            dataType=DataType.INT,   description="Total de faltas do aluno"),
                Column(name="PERFIL",            dataType=DataType.STRING,description="Perfil comportamental/acadêmico do aluno"),
                Column(name="data_ingestao",     dataType=DataType.STRING,description="Timestamp de ingestão do registro"),
                Column(name="fonte",             dataType=DataType.STRING,description="Origem dos dados brutos"),
            ],
        ),

        # ── SILVER ──────────────────────────────────────────────────────────
        CreateTableRequest(
            name="alunos_transformado",
            displayName="Alunos Transformado (Silver)",
            description=(
                "Dados de alunos após tratamento: nulos preenchidos, campos "
                "padronizados e colunas derivadas adicionadas. 199 registros."
            ),
            tableType=TableType.Regular,
            databaseSchema=schema_fqn,
            columns=[
                Column(name="MATRICULA",          dataType=DataType.INT,   description="Identificador único do aluno"),
                Column(name="NOME",               dataType=DataType.STRING,description="Nome completo do aluno — dado PII"),
                Column(name="REPROVACOES_MAT_1",  dataType=DataType.INT),
                Column(name="REPROVACOES_MAT_2",  dataType=DataType.INT),
                Column(name="REPROVACOES_MAT_3",  dataType=DataType.INT),
                Column(name="REPROVACOES_MAT_4",  dataType=DataType.INT),
                Column(name="NOTA_MAT_1",         dataType=DataType.FLOAT),
                Column(name="NOTA_MAT_2",         dataType=DataType.FLOAT),
                Column(name="NOTA_MAT_3",         dataType=DataType.FLOAT),
                Column(name="NOTA_MAT_4",         dataType=DataType.FLOAT, description="Nulos preenchidos na camada silver"),
                Column(name="INGLES",             dataType=DataType.INT,   description="Nulos preenchidos e convertido para int"),
                Column(name="H_AULA_PRES",        dataType=DataType.INT),
                Column(name="TAREFAS_ONLINE",     dataType=DataType.INT),
                Column(name="FALTAS",             dataType=DataType.INT),
                Column(name="PERFIL",             dataType=DataType.STRING),
                Column(name="data_ingestao",      dataType=DataType.STRING),
                Column(name="fonte",              dataType=DataType.STRING),
                Column(name="MEDIA_GERAL",        dataType=DataType.FLOAT, description="Média aritmética das 4 matérias — coluna derivada"),
                Column(name="TAXA_PRESENCA",      dataType=DataType.FLOAT, description="Percentual de presença — coluna derivada"),
                Column(name="INDICE_ENGAJAMENTO", dataType=DataType.FLOAT, description="Índice composto de engajamento — coluna derivada"),
                Column(name="TOTAL_REPROVACOES",  dataType=DataType.INT,   description="Soma total de reprovações — coluna derivada"),
                Column(name="STATUS_MAT_1",       dataType=DataType.STRING,description="Aprovado/Reprovado na matéria 1"),
                Column(name="STATUS_MAT_2",       dataType=DataType.STRING,description="Aprovado/Reprovado na matéria 2"),
                Column(name="STATUS_MAT_3",       dataType=DataType.STRING,description="Aprovado/Reprovado na matéria 3"),
                Column(name="STATUS_MAT_4",       dataType=DataType.STRING,description="Aprovado/Reprovado na matéria 4"),
                Column(name="data_transformacao", dataType=DataType.STRING,description="Timestamp da transformação silver"),
            ],
        ),

        # ── GOLD ────────────────────────────────────────────────────────────
        CreateTableRequest(
            name="kpis_dashboard",
            displayName="KPIs Dashboard (Gold)",
            description="KPIs consolidados para uso em dashboard executivo. 11 métricas.",
            tableType=TableType.Regular,
            databaseSchema=schema_fqn,
            columns=[
                Column(name="metrica",    dataType=DataType.STRING,description="Nome da métrica"),
                Column(name="valor",      dataType=DataType.FLOAT, description="Valor numérico da métrica"),
                Column(name="percentual", dataType=DataType.FLOAT, description="Percentual — pode ser nulo"),
                Column(name="categoria",  dataType=DataType.STRING,description="Categoria da métrica"),
            ],
        ),
        CreateTableRequest(
            name="analise_risco",
            displayName="Análise de Risco (Gold)",
            description="Análise de risco de evasão segmentada por perfil. 7 registros.",
            tableType=TableType.Regular,
            databaseSchema=schema_fqn,
            columns=[
                Column(name="analise",    dataType=DataType.STRING,description="Tipo de análise de risco"),
                Column(name="quantidade", dataType=DataType.FLOAT, description="Quantidade de alunos no grupo"),
                Column(name="percentual", dataType=DataType.FLOAT, description="Percentual do grupo em relação ao total"),
                Column(name="detalhes",   dataType=DataType.STRING,description="Descrição detalhada do grupo de risco"),
            ],
        ),
        CreateTableRequest(
            name="analise_engajamento",
            displayName="Análise de Engajamento (Gold)",
            description="Índices de engajamento dos alunos por categoria. 8 registros.",
            tableType=TableType.Regular,
            databaseSchema=schema_fqn,
            columns=[
                Column(name="analise",   dataType=DataType.STRING,description="Dimensão de engajamento analisada"),
                Column(name="valor",     dataType=DataType.FLOAT, description="Valor do índice"),
                Column(name="categoria", dataType=DataType.STRING,description="Categoria do engajamento"),
            ],
        ),
        CreateTableRequest(
            name="insights",
            displayName="Insights (Gold)",
            description="Insights priorizados gerados pelo pipeline. 6 registros.",
            tableType=TableType.Regular,
            databaseSchema=schema_fqn,
            columns=[
                Column(name="insight",    dataType=DataType.STRING,description="Descrição do insight gerado"),
                Column(name="prioridade", dataType=DataType.STRING,description="Nível de prioridade: Alta / Média / Baixa"),
                Column(name="valor",      dataType=DataType.FLOAT, description="Valor quantitativo associado ao insight"),
                Column(name="detalhes",   dataType=DataType.STRING,description="Detalhamento do insight"),
            ],
        ),
    ]

    for tabela in tabelas:
        result = metadata.create_or_update(tabela)
        log.info("Tabela registrada: %s", str(result.name))

    log.info("✅ %d tabelas registradas (incluindo curso_txt).", len(tabelas))


# ─── Task 2: Registrar Linhagem ───────────────────────────────────────────────

def registrar_lineage(**context):
    """
    Registra linhagem completa:
      curso_txt → alunos_raw (bronze) → alunos_transformado (silver) → 4 tabelas gold

    A aresta curso_txt → bronze também é criada automaticamente pelo Airflow
    Lineage Backend quando a DAG 1-dag_bronze.py é executada (via inlets/outlets
    com Asset URI). Este registro manual garante que o grafo apareça mesmo que
    o backend ainda não tenha processado a execução.
    """
    from metadata.generated.schema.api.lineage.addLineage import AddLineageRequest
    from metadata.generated.schema.entity.data.table import Table
    from metadata.generated.schema.type.entityLineage import EntitiesEdge, LineageDetails
    from metadata.generated.schema.type.entityReference import EntityReference

    metadata = get_metadata_client()

    def get_table(name):
        fqn = f"{FQN_BASE}.{name}"
        t = metadata.get_by_name(entity=Table, fqn=fqn)
        if not t:
            raise ValueError(f"Tabela não encontrada: {fqn}")
        return t

    fonte  = get_table("curso_txt")
    bronze = get_table("alunos_raw")
    silver = get_table("alunos_transformado")
    gold_tables = {
        "kpis_dashboard":      get_table("kpis_dashboard"),
        "analise_risco":       get_table("analise_risco"),
        "analise_engajamento": get_table("analise_engajamento"),
        "insights":            get_table("insights"),
    }

    # ── fonte CSV → bronze ───────────────────────────────────────────────────
    metadata.add_lineage(AddLineageRequest(edge=EntitiesEdge(
        fromEntity=EntityReference(id=fonte.id,  type="table"),
        toEntity=EntityReference(id=bronze.id,   type="table"),
        lineageDetails=LineageDetails(description=(
            "Ingestão do arquivo CSV fonte (curso.txt) para a camada Bronze. "
            "Adiciona colunas de metadados: data_ingestao e fonte."
        )),
    )))
    log.info("Linhagem: curso_txt → alunos_raw")

    # ── bronze → silver ──────────────────────────────────────────────────────
    metadata.add_lineage(AddLineageRequest(edge=EntitiesEdge(
        fromEntity=EntityReference(id=bronze.id, type="table"),
        toEntity=EntityReference(id=silver.id,   type="table"),
        lineageDetails=LineageDetails(description=(
            "Transformações: tratamento de nulos, padronização de campos, "
            "criação de colunas derivadas (MEDIA_GERAL, TAXA_PRESENCA, "
            "INDICE_ENGAJAMENTO, TOTAL_REPROVACOES, STATUS_MAT_*)."
        )),
    )))
    log.info("Linhagem: alunos_raw → alunos_transformado")

    # ── silver → gold (4 tabelas) ────────────────────────────────────────────
    descricoes = {
        "kpis_dashboard":      "Agregações por perfil e cálculo de 11 KPIs executivos.",
        "analise_risco":       "Segmentação de risco de evasão por perfil e reprovações.",
        "analise_engajamento": "Cálculo de índices de engajamento por dimensão.",
        "insights":            "Geração de 6 insights priorizados (Alta/Média/Baixa).",
    }
    for nome, gold in gold_tables.items():
        metadata.add_lineage(AddLineageRequest(edge=EntitiesEdge(
            fromEntity=EntityReference(id=silver.id, type="table"),
            toEntity=EntityReference(id=gold.id,    type="table"),
            lineageDetails=LineageDetails(description=descricoes[nome]),
        )))
        log.info("Linhagem: alunos_transformado → %s", nome)

    log.info("✅ Linhagem completa registrada (curso_txt → bronze → silver → 4 gold).")


# ─── Task 3: Aplicar Governança ───────────────────────────────────────────────

def aplicar_governanca(**context):
    """
    Owner, tags PII/Tier e descrições em todas as tabelas.
    Inclui curso_txt com PII.Sensitive e Tier3 (dado externo/fonte).
    """
    from metadata.generated.schema.entity.data.table import Table
    from metadata.generated.schema.api.classification.createTag import CreateTagRequest
    from metadata.generated.schema.api.classification.createClassification import CreateClassificationRequest
    from metadata.generated.schema.entity.classification.classification import Classification
    from metadata.generated.schema.entity.classification.tag import Tag
    from metadata.generated.schema.entity.teams.user import User
    from metadata.generated.schema.type.entityReference import EntityReference

    metadata = get_metadata_client()

    # Criar classificações e tags se não existirem
    for cls_name, tags in {"PII": ["Sensitive"], "Tier": ["Tier1", "Tier2", "Tier3"]}.items():
        try:
            if not metadata.get_by_name(entity=Classification, fqn=cls_name):
                metadata.create_or_update(CreateClassificationRequest(
                    name=cls_name, description=f"Classificação {cls_name}",
                ))
        except Exception as e:
            log.warning("Classificação %s: %s", cls_name, e)
        for tag_name in tags:
            try:
                if not metadata.get_by_name(entity=Tag, fqn=f"{cls_name}.{tag_name}"):
                    metadata.create_or_update(CreateTagRequest(
                        classification=cls_name, name=tag_name,
                        description=f"Tag {tag_name}",
                    ))
            except Exception as e:
                log.warning("Tag %s.%s: %s", cls_name, tag_name, e)

    owner_user = metadata.get_by_name(entity=User, fqn="admin")
    owner_ref  = EntityReference(id=owner_user.id, type="user") if owner_user else None

    config = {
        # Fonte CSV — raiz do grafo, contém PII (nomes dos alunos)
        "curso_txt":           {"pii": True,  "tier": "Tier.Tier3", "descricao": "Arquivo CSV fonte do pipeline. Origem de todos os dados de alunos. Contém PII (NOME)."},
        # Bronze — cópia fiel da fonte + metadados de ingestão
        "alunos_raw":          {"pii": True,  "tier": "Tier.Tier3", "descricao": "Dados brutos de alunos — camada Bronze. 199 registros. Contém PII."},
        # Silver — dado tratado, ainda contém PII (NOME)
        "alunos_transformado": {"pii": True,  "tier": "Tier.Tier2", "descricao": "Dados tratados com colunas derivadas — camada Silver. 199 registros. Contém PII."},
        # Gold — agregados, sem PII
        "kpis_dashboard":      {"pii": False, "tier": "Tier.Tier1", "descricao": "KPIs para dashboard executivo — camada Gold. 11 métricas."},
        "analise_risco":       {"pii": False, "tier": "Tier.Tier1", "descricao": "Risco de evasão por perfil — camada Gold. 7 registros."},
        "analise_engajamento": {"pii": False, "tier": "Tier.Tier1", "descricao": "Índices de engajamento — camada Gold. 8 registros."},
        "insights":            {"pii": False, "tier": "Tier.Tier1", "descricao": "Insights priorizados — camada Gold. 6 registros."},
    }

    for nome, cfg in config.items():
        tabela = metadata.get_by_name(entity=Table, fqn=f"{FQN_BASE}.{nome}")
        if not tabela:
            log.warning("Tabela não encontrada: %s", nome)
            continue
        try:
            metadata.patch_description(entity=Table, source=tabela, description=cfg["descricao"])
            metadata.patch_tag(entity=Table, source=tabela, tag_fqn=cfg["tier"], is_suggested=False)
            if cfg["pii"]:
                metadata.patch_tag(entity=Table, source=tabela, tag_fqn="PII.Sensitive", is_suggested=False)
            if owner_ref:
                metadata.patch_owner(entity=Table, source=tabela, owner=owner_ref)
            log.info("Governança aplicada: %s", nome)
        except Exception as e:
            log.warning("Erro em %s: %s", nome, e)

    log.info("✅ Governança aplicada (7 tabelas, incluindo curso_txt).")


# ─── Task 4: Registrar Glossário ─────────────────────────────────────────────

def registrar_glossario(**context):
    """Cria termos de negócio no Glossário Educação."""
    from metadata.generated.schema.api.data.createGlossary import CreateGlossaryRequest
    from metadata.generated.schema.api.data.createGlossaryTerm import CreateGlossaryTermRequest
    from metadata.generated.schema.entity.data.glossary import Glossary
    from metadata.generated.schema.type.entityReference import EntityReference

    metadata = get_metadata_client()

    glossario_nome = "Glossario_Educacao"
    glossario = metadata.get_by_name(entity=Glossary, fqn=glossario_nome)
    if not glossario:
        glossario = metadata.create_or_update(CreateGlossaryRequest(
            name=glossario_nome,
            displayName="Glossário Educação",
            description="Termos de negócio do domínio educacional.",
        ))
        log.info("Glossário criado: %s", glossario_nome)

    glossario_ref = EntityReference(id=glossario.id, type="glossary")

    termos = [
        {"name": "MediaGeral",        "displayName": "Média Geral",           "description": "Média aritmética das notas nas 4 matérias. Calculada na Silver."},
        {"name": "TaxaPresenca",      "displayName": "Taxa de Presença",      "description": "Percentual de presença em relação às horas totais de aula."},
        {"name": "IndiceEngajamento", "displayName": "Índice de Engajamento", "description": "Índice composto: presença + conclusão de tarefas online."},
        {"name": "RiscoEvasao",       "displayName": "Risco de Evasão",       "description": "Risco de abandono baseado em reprovações, faltas e engajamento."},
        {"name": "PerfilAluno",       "displayName": "Perfil do Aluno",       "description": "Segmentação comportamental/acadêmica usada nas agregações gold."},
    ]

    for termo in termos:
        try:
            metadata.create_or_update(CreateGlossaryTermRequest(
                glossary=glossario_ref,
                name=termo["name"],
                displayName=termo["displayName"],
                description=termo["description"],
            ))
            log.info("Termo registrado: %s", termo["name"])
        except Exception as e:
            log.warning("Erro no termo %s: %s", termo["name"], e)

    log.info("✅ Glossário registrado.")


# ─── Task 5: Vincular Glossário às Colunas ────────────────────────────────────

def vincular_glossario_colunas(**context):
    """
    Vincula termos do Glossário Educação às colunas relevantes.
    Inclui vínculo de PerfilAluno na tabela fonte (curso_txt).
    Aparece em: Governar > Glossário (coluna com termo vinculado).
    """
    from metadata.generated.schema.entity.data.table import Table

    metadata = get_metadata_client()

    vinculos = [
        # Silver — colunas derivadas
        {"tabela": "alunos_transformado", "coluna": "MEDIA_GERAL",        "termo": "Glossario_Educacao.MediaGeral"},
        {"tabela": "alunos_transformado", "coluna": "TAXA_PRESENCA",      "termo": "Glossario_Educacao.TaxaPresenca"},
        {"tabela": "alunos_transformado", "coluna": "INDICE_ENGAJAMENTO", "termo": "Glossario_Educacao.IndiceEngajamento"},
        {"tabela": "alunos_transformado", "coluna": "TOTAL_REPROVACOES",  "termo": "Glossario_Educacao.RiscoEvasao"},
        {"tabela": "alunos_transformado", "coluna": "PERFIL",             "termo": "Glossario_Educacao.PerfilAluno"},
        # Bronze
        {"tabela": "alunos_raw",          "coluna": "PERFIL",             "termo": "Glossario_Educacao.PerfilAluno"},
        # Fonte CSV — raiz do grafo
        {"tabela": "curso_txt",           "coluna": "PERFIL",             "termo": "Glossario_Educacao.PerfilAluno"},
    ]

    for v in vinculos:
        try:
            tabela = metadata.get_by_name(
                entity=Table, fqn=f"{FQN_BASE}.{v['tabela']}"
            )
            if not tabela:
                log.warning("Tabela não encontrada: %s", v["tabela"])
                continue
            metadata.patch_column_tag(
                entity=Table,
                source=tabela,
                column_name=v["coluna"],
                tag_fqn=v["termo"],
                is_suggested=False,
            )
            log.info("Glossário vinculado: %s.%s → %s", v["tabela"], v["coluna"], v["termo"])
        except Exception as e:
            log.warning("Erro ao vincular %s.%s: %s", v["tabela"], v["coluna"], e)

    log.info("✅ Glossário vinculado às colunas (fonte, bronze, silver).")


# ─── Task 6: Criar Domínio e Produto de Dados ─────────────────────────────────

def criar_dominio_produto(**context):
    """
    Cria o Domínio 'Educação' e o Produto de Dados 'analytics-desempenho-alunos'
    agrupando as 4 tabelas gold.
    Aparece em: Domínios > Domínios e Domínios > Produtos de Dados.
    """
    from metadata.generated.schema.api.domains.createDomain import CreateDomainRequest
    from metadata.generated.schema.api.domains.createDataProduct import CreateDataProductRequest
    from metadata.generated.schema.entity.domains.domain import Domain, DomainType

    metadata = get_metadata_client()

    dominio_nome = "Educacao"
    try:
        dominio = metadata.get_by_name(entity=Domain, fqn=dominio_nome)
        if not dominio:
            dominio = metadata.create_or_update(CreateDomainRequest(
                name=dominio_nome,
                displayName="Educação",
                description=(
                    "Domínio de dados educacionais. Engloba o pipeline medallion "
                    "de alunos: ingestão, transformação, agregação e governança."
                ),
                domainType=DomainType.Consumer_aligned,
            ))
            log.info("Domínio criado: %s", dominio_nome)
        else:
            log.info("Domínio já existe: %s", dominio_nome)
    except Exception as e:
        log.warning("Erro ao criar domínio: %s", e)
        return

    try:
        metadata.create_or_update(CreateDataProductRequest(
            name="analytics-desempenho-alunos",
            displayName="Analytics de Desempenho de Alunos",
            description=(
                "Produto de dados com KPIs, análise de risco, engajamento e insights "
                "dos alunos. Gerado pelo pipeline medallion bronze → silver → gold."
            ),
            domain=dominio.fullyQualifiedName,
            assets=[
                f"{FQN_BASE}.kpis_dashboard",
                f"{FQN_BASE}.analise_risco",
                f"{FQN_BASE}.analise_engajamento",
                f"{FQN_BASE}.insights",
            ],
            experts=["admin"],
        ))
        log.info("Produto de dados criado: analytics-desempenho-alunos")
    except Exception as e:
        log.warning("Erro ao criar produto de dados: %s", e)

    log.info("✅ Domínio e Produto de Dados registrados.")


# ─── Task 7: Registrar Métrica de Negócio ────────────────────────────────────

def registrar_metrica_negocio(**context):
    """
    Registra métricas de negócio calculadas pelo pipeline.
    Aparece em: Governar > Métricas.
    """
    from metadata.generated.schema.api.data.createMetric import CreateMetricRequest
    from metadata.generated.schema.entity.data.metric import MetricGranularity

    metadata = get_metadata_client()

    metricas = [
        {
            "name":        "media-geral-turma",
            "displayName": "Média Geral da Turma",
            "description": "Média aritmética de todos os alunos nas 4 matérias.",
            "expression":  "SELECT AVG(MEDIA_GERAL) FROM alunos_transformado",
            "unit":        "Pontos (0-10)",
        },
        {
            "name":        "taxa-presenca-media",
            "displayName": "Taxa de Presença Média",
            "description": "Percentual médio de presença de todos os alunos.",
            "expression":  "SELECT AVG(TAXA_PRESENCA) FROM alunos_transformado",
            "unit":        "Percentual (%)",
        },
        {
            "name":        "indice-engajamento-medio",
            "displayName": "Índice de Engajamento Médio",
            "description": "Índice médio de engajamento da turma (presença + tarefas).",
            "expression":  "SELECT AVG(INDICE_ENGAJAMENTO) FROM alunos_transformado",
            "unit":        "Índice (0-100)",
        },
        {
            "name":        "total-alunos-risco",
            "displayName": "Total de Alunos em Risco",
            "description": "Alunos com perfil DIFICULDADE ou múltiplas reprovações.",
            "expression":  "SELECT COUNT(*) FROM alunos_transformado WHERE PERFIL = 'DIFICULDADE' OR TOTAL_REPROVACOES >= 3",
            "unit":        "Alunos",
        },
    ]

    for m in metricas:
        try:
            metadata.create_or_update(CreateMetricRequest(
                name=m["name"],
                displayName=m["displayName"],
                description=m["description"],
                metricExpression={"language": "SQL", "code": m["expression"]},
                granularity=MetricGranularity.Day,
                unitOfMeasurement=m["unit"],
            ))
            log.info("Métrica registrada: %s", m["name"])
        except Exception as e:
            log.warning("Erro na métrica %s: %s", m["name"], e)

    log.info("✅ Métricas de negócio registradas.")


# ─── Task 8: Criar Alertas de Qualidade ──────────────────────────────────────

def criar_alertas(**context):
    """
    Cria alertas automáticos para falhas de qualidade e mudanças de schema.
    Aparece em: Observabilidade > Alertas.
    """
    from metadata.generated.schema.api.events.createEventSubscription import (
        CreateEventSubscriptionRequest,
    )

    metadata = get_metadata_client()

    alertas = [
        {
            "name":        "alerta-falha-qualidade-pipeline-alunos",
            "displayName": "Alerta: Falha de Qualidade — Pipeline Alunos",
            "description": "Dispara quando um teste de qualidade falha nas tabelas Tier1 do pipeline.",
            "resources":   ["testCase"],
            "eventType":   "TestCaseFailed",
        },
        {
            "name":        "alerta-mudanca-schema-silver",
            "displayName": "Alerta: Mudança de Schema — Silver",
            "description": "Dispara quando colunas são adicionadas ou removidas de alunos_transformado.",
            "resources":   ["table"],
            "eventType":   "EntityUpdated",
        },
        {
            "name":        "alerta-nova-tabela-gold",
            "displayName": "Alerta: Nova Tabela — Gold",
            "description": "Dispara quando uma nova tabela é criada na camada gold.",
            "resources":   ["table"],
            "eventType":   "EntityCreated",
        },
    ]

    for alerta in alertas:
        try:
            metadata.create_or_update(CreateEventSubscriptionRequest(
                name=alerta["name"],
                displayName=alerta["displayName"],
                description=alerta["description"],
                alertType="Notification",
                trigger={"triggerType": "RealTime"},
                filteringRules={
                    "resources": alerta["resources"],
                    "rules": [{
                        "name":      "eventType",
                        "effect":    "include",
                        "condition": f"matchAnyEventType('{alerta['eventType']}')",
                    }],
                },
                subscriptions=[{
                    "category": "Users",
                    "type":     "ActivityFeed",
                    "receivers": ["admin"],
                }],
            ))
            log.info("Alerta criado: %s", alerta["name"])
        except Exception as e:
            log.warning("Erro ao criar alerta %s: %s", alerta["name"], e)

    log.info("✅ Alertas de qualidade configurados.")


# ─── DAG Definition ───────────────────────────────────────────────────────────

default_args = {
    "owner":            "engenharia.dados@empresa.com",
    "depends_on_past":  False,
    "email_on_failure": False,
    "email_on_retry":   False,
    "retries":          1,
}

with DAG(
    dag_id="7_catalog_openmetadata",
    default_args=default_args,
    description=(
        "Cataloga tabelas (incluindo curso_txt fonte), linhagem completa, "
        "governança, glossário, domínios, métricas e alertas no OpenMetadata."
    ),
    schedule=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["catalog", "governanca", "openmetadata"],
) as dag:

    t0 = PythonOperator(task_id="criar_servico_schema",       python_callable=criar_servico_schema,       doc_md="Cria serviço, database e schema.")
    t1 = PythonOperator(task_id="registrar_tabelas",          python_callable=registrar_tabelas,          doc_md="Cria/atualiza 7 tabelas: curso_txt + 6 camadas medallion.")
    t2 = PythonOperator(task_id="registrar_lineage",          python_callable=registrar_lineage,          doc_md="Linhagem: curso_txt → bronze → silver → 4 gold.")
    t3 = PythonOperator(task_id="aplicar_governanca",         python_callable=aplicar_governanca,         doc_md="Tags PII/Tier, owner e descrições em 7 tabelas.")
    t4 = PythonOperator(task_id="registrar_glossario",        python_callable=registrar_glossario,        doc_md="5 termos no Glossário Educação.")
    t5 = PythonOperator(task_id="vincular_glossario_colunas", python_callable=vincular_glossario_colunas, doc_md="Vincula termos às colunas da fonte, bronze e silver.")
    t6 = PythonOperator(task_id="criar_dominio_produto",      python_callable=criar_dominio_produto,      doc_md="Domínio Educação + Produto de Dados gold.")
    t7 = PythonOperator(task_id="registrar_metrica_negocio",  python_callable=registrar_metrica_negocio,  doc_md="4 métricas de negócio em Governar > Métricas.")
    t8 = PythonOperator(task_id="criar_alertas",              python_callable=criar_alertas,              doc_md="3 alertas em Observabilidade > Alertas.")

    t0 >> t1 >> t2 >> t3 >> t4 >> t5 >> t6 >> t7 >> t8
