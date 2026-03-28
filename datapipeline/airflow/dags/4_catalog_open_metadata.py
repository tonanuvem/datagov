"""
DAG de Catalogação no OpenMetadata — Versão Didática Simplificada
=================================================================
Demonstra os conceitos principais de governança de dados:

  1. registrar_tabelas       → cadastra as tabelas do pipeline medallion
  2. registrar_lineage       → curso_txt → bronze → silver → gold
  3. aplicar_governanca      → owner, tags PII/Tier e descrições
  4. registrar_glossario     → termos de negócio (Glossário Educação)
  5. criar_dominio_produto   → Domínio Educação + Produto de Dados

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
AUTENTICAÇÃO — Como configurar a Airflow Connection:

  Airflow UI → Admin → Connections → [+] Add a new record
    Connection Id : openmetadata_default
    Connection Type: HTTP
    Host          : openmetadata-server
    Port          : 8585
    Password      : <seu JWT token>

  Ou via CLI:
    airflow connections add openmetadata_default \
        --conn-type http \
        --conn-host openmetadata-server \
        --conn-port 8585 \
        --conn-password "<jwt_token>"

  Por que Connection e não variável de ambiente?
  → O token fica criptografado no banco do Airflow (Fernet).
  → Evita o erro "Not Authorized! The given token does not match
    the current bot's token!" causado pela variável não estar
    disponível no worker no momento da execução.
  → Mesmo padrão usado para conexões com bancos e APIs externas.
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

Grafo de linhagem esperado no OpenMetadata:
  curso_txt (CSV)
      └─► alunos_raw (Bronze)
              └─► alunos_transformado (Silver)
                      ├─► kpis_dashboard
                      ├─► analise_risco
                      ├─► analise_engajamento
                      └─► insights
"""

from __future__ import annotations

import logging
from datetime import datetime

from airflow import DAG
from airflow.hooks.base import BaseHook
from airflow.operators.python import PythonOperator

# ─── Configuração ─────────────────────────────────────────────────────────────

OM_CONN_ID = "openmetadata_default"   # ID da Airflow Connection
SERVICE    = "pipeline_alunos"
DATABASE   = "educacao"
SCHEMA     = "camadas"
FQN_BASE   = f"{SERVICE}.{DATABASE}.{SCHEMA}"

log = logging.getLogger(__name__)


# ─── Helper: lê credenciais da Airflow Connection ─────────────────────────────

def get_om_config() -> tuple[str, str]:
    """
    Lê host e JWT token da Airflow Connection 'openmetadata_default'.

    Retorna (host_url, jwt_token).

    Usando BaseHook.get_connection() o token é resolvido em tempo de
    execução do worker — nunca em tempo de parsing da DAG —
    evitando erros de autenticação por token ausente ou inválido.
    """
    conn = BaseHook.get_connection(OM_CONN_ID)
    host = f"http://{conn.host}:{conn.port}/api"
    token = conn.password
    if not token:
        raise ValueError(
            f"JWT token não encontrado na Connection '{OM_CONN_ID}'. "
            "Configure o campo Password com o token do OpenMetadata."
        )
    return host, token


def get_client():
    """Retorna o cliente autenticado do OpenMetadata."""
    from metadata.generated.schema.entity.services.connections.metadata.openMetadataConnection import (
        OpenMetadataConnection,
    )
    from metadata.generated.schema.security.client.openMetadataJWTClientConfig import (
        OpenMetadataJWTClientConfig,
    )
    from metadata.ingestion.ometa.ometa_api import OpenMetadata

    host, token = get_om_config()
    return OpenMetadata(OpenMetadataConnection(
        hostPort=host,
        authProvider="openmetadata",
        securityConfig=OpenMetadataJWTClientConfig(jwtToken=token),
    ))


# ─── Helper: colunas compartilhadas entre Fonte, Bronze e Silver ──────────────

def _colunas_base():
    from metadata.generated.schema.entity.data.table import Column, DataType
    return [
        Column(name="MATRICULA",         dataType=DataType.INT,    description="Identificador único do aluno"),
        Column(name="NOME",              dataType=DataType.STRING, description="Nome completo — dado PII"),
        Column(name="REPROVACOES_MAT_1", dataType=DataType.INT),
        Column(name="REPROVACOES_MAT_2", dataType=DataType.INT),
        Column(name="REPROVACOES_MAT_3", dataType=DataType.INT),
        Column(name="REPROVACOES_MAT_4", dataType=DataType.INT),
        Column(name="NOTA_MAT_1",        dataType=DataType.FLOAT),
        Column(name="NOTA_MAT_2",        dataType=DataType.FLOAT),
        Column(name="NOTA_MAT_3",        dataType=DataType.FLOAT),
        Column(name="NOTA_MAT_4",        dataType=DataType.FLOAT),
        Column(name="INGLES",            dataType=DataType.FLOAT),
        Column(name="H_AULA_PRES",       dataType=DataType.INT),
        Column(name="TAREFAS_ONLINE",    dataType=DataType.INT),
        Column(name="FALTAS",            dataType=DataType.INT),
        Column(name="PERFIL",            dataType=DataType.STRING),
    ]


# ─── Task 1: Registrar Tabelas ────────────────────────────────────────────────

def registrar_tabelas(**_):
    """
    Registra todas as tabelas do pipeline no catálogo.

    Conceito: cada tabela vira um 'asset' no OpenMetadata com nome,
    descrição e schema (colunas com tipos e descrições).

    Também garante que o serviço, database e schema existam antes
    de tentar criar as tabelas (create_or_update é idempotente).
    """
    from metadata.generated.schema.api.data.createDatabase import CreateDatabaseRequest
    from metadata.generated.schema.api.data.createDatabaseSchema import CreateDatabaseSchemaRequest
    from metadata.generated.schema.api.data.createTable import CreateTableRequest
    from metadata.generated.schema.api.services.createDatabaseService import CreateDatabaseServiceRequest
    from metadata.generated.schema.entity.data.table import Column, DataType, TableType
    from metadata.generated.schema.entity.services.connections.database.customDatabaseConnection import (
        CustomDatabaseConnection,
    )
    from metadata.generated.schema.entity.services.databaseService import (
        DatabaseConnection,
        DatabaseServiceType,
    )

    md = get_client()

    # Hierarquia: Serviço → Database → Schema (idempotente)
    md.create_or_update(CreateDatabaseServiceRequest(
        name=SERVICE,
        serviceType=DatabaseServiceType.CustomDatabase,
        connection=DatabaseConnection(config=CustomDatabaseConnection(
            type="CustomDatabase",
            sourcePythonClass="metadata.ingestion.source.database.customdatabase.metadata.CustomDatabaseSource",
        )),
    ))
    md.create_or_update(CreateDatabaseRequest(name=DATABASE, service=SERVICE))
    md.create_or_update(CreateDatabaseSchemaRequest(name=SCHEMA, database=f"{SERVICE}.{DATABASE}"))

    tabelas = [

        # FONTE — CSV de origem, raiz do grafo de linhagem
        CreateTableRequest(
            name="curso_txt",
            displayName="Fonte: curso.txt (CSV)",
            description="Arquivo CSV de origem. Raiz do grafo: curso_txt → Bronze → Silver → Gold.",
            tableType=TableType.Regular,
            databaseSchema=FQN_BASE,
            columns=_colunas_base(),
        ),

        # BRONZE — dados brutos + metadados de ingestão
        CreateTableRequest(
            name="alunos_raw",
            displayName="Alunos Raw (Bronze)",
            description="Dados brutos ingeridos da fonte. 199 registros.",
            tableType=TableType.Regular,
            databaseSchema=FQN_BASE,
            columns=_colunas_base() + [
                Column(name="data_ingestao", dataType=DataType.STRING, description="Timestamp de ingestão"),
                Column(name="fonte",         dataType=DataType.STRING, description="Origem dos dados"),
            ],
        ),

        # SILVER — dados tratados + colunas derivadas
        CreateTableRequest(
            name="alunos_transformado",
            displayName="Alunos Transformado (Silver)",
            description="Dados tratados com nulos preenchidos e colunas derivadas. 199 registros.",
            tableType=TableType.Regular,
            databaseSchema=FQN_BASE,
            columns=_colunas_base() + [
                Column(name="data_ingestao",      dataType=DataType.STRING),
                Column(name="fonte",              dataType=DataType.STRING),
                Column(name="MEDIA_GERAL",        dataType=DataType.FLOAT,  description="Média das 4 matérias — coluna derivada"),
                Column(name="TAXA_PRESENCA",      dataType=DataType.FLOAT,  description="% de presença — coluna derivada"),
                Column(name="INDICE_ENGAJAMENTO", dataType=DataType.FLOAT,  description="Índice composto — coluna derivada"),
                Column(name="TOTAL_REPROVACOES",  dataType=DataType.INT,    description="Soma de reprovações — coluna derivada"),
                Column(name="STATUS_MAT_1",       dataType=DataType.STRING, description="Aprovado/Reprovado na matéria 1"),
                Column(name="STATUS_MAT_2",       dataType=DataType.STRING, description="Aprovado/Reprovado na matéria 2"),
                Column(name="STATUS_MAT_3",       dataType=DataType.STRING, description="Aprovado/Reprovado na matéria 3"),
                Column(name="STATUS_MAT_4",       dataType=DataType.STRING, description="Aprovado/Reprovado na matéria 4"),
                Column(name="data_transformacao", dataType=DataType.STRING, description="Timestamp da transformação"),
            ],
        ),

        # GOLD — tabelas analíticas finais
        CreateTableRequest(
            name="kpis_dashboard",
            displayName="KPIs Dashboard (Gold)",
            description="11 KPIs consolidados para dashboard executivo.",
            tableType=TableType.Regular,
            databaseSchema=FQN_BASE,
            columns=[
                Column(name="metrica",    dataType=DataType.STRING),
                Column(name="valor",      dataType=DataType.FLOAT),
                Column(name="percentual", dataType=DataType.FLOAT),
                Column(name="categoria",  dataType=DataType.STRING),
            ],
        ),
        CreateTableRequest(
            name="analise_risco",
            displayName="Análise de Risco (Gold)",
            description="Risco de evasão segmentado por perfil. 7 registros.",
            tableType=TableType.Regular,
            databaseSchema=FQN_BASE,
            columns=[
                Column(name="analise",    dataType=DataType.STRING),
                Column(name="quantidade", dataType=DataType.FLOAT),
                Column(name="percentual", dataType=DataType.FLOAT),
                Column(name="detalhes",   dataType=DataType.STRING),
            ],
        ),
        CreateTableRequest(
            name="analise_engajamento",
            displayName="Análise de Engajamento (Gold)",
            description="Índices de engajamento por categoria. 8 registros.",
            tableType=TableType.Regular,
            databaseSchema=FQN_BASE,
            columns=[
                Column(name="analise",   dataType=DataType.STRING),
                Column(name="valor",     dataType=DataType.FLOAT),
                Column(name="categoria", dataType=DataType.STRING),
            ],
        ),
        CreateTableRequest(
            name="insights",
            displayName="Insights (Gold)",
            description="6 insights priorizados gerados pelo pipeline.",
            tableType=TableType.Regular,
            databaseSchema=FQN_BASE,
            columns=[
                Column(name="insight",    dataType=DataType.STRING),
                Column(name="prioridade", dataType=DataType.STRING),
                Column(name="valor",      dataType=DataType.FLOAT),
                Column(name="detalhes",   dataType=DataType.STRING),
            ],
        ),
    ]

    for tabela in tabelas:
        md.create_or_update(tabela)
        log.info("✔ Tabela registrada: %s", tabela.name)

    log.info("✅ %d tabelas registradas.", len(tabelas))


# ─── Task 2: Registrar Linhagem ───────────────────────────────────────────────

def registrar_lineage(**_):
    """
    Registra o grafo de linhagem completo.

    Conceito: linhagem mostra a origem e o destino dos dados,
    permitindo análise de impacto e rastreabilidade.
    """
    from metadata.generated.schema.api.lineage.addLineage import AddLineageRequest
    from metadata.generated.schema.entity.data.table import Table
    from metadata.generated.schema.type.entityLineage import EntitiesEdge, LineageDetails
    from metadata.generated.schema.type.entityReference import EntityReference

    md = get_client()

    def ref(nome):
        t = md.get_by_name(entity=Table, fqn=f"{FQN_BASE}.{nome}")
        if not t:
            raise ValueError(f"Tabela não encontrada: {FQN_BASE}.{nome}")
        return EntityReference(id=t.id, type="table")

    def aresta(origem, destino, descricao):
        md.add_lineage(AddLineageRequest(edge=EntitiesEdge(
            fromEntity=ref(origem),
            toEntity=ref(destino),
            lineageDetails=LineageDetails(description=descricao),
        )))
        log.info("✔ Linhagem: %s → %s", origem, destino)

    aresta("curso_txt",           "alunos_raw",          "Ingestão do CSV fonte para a camada Bronze.")
    aresta("alunos_raw",          "alunos_transformado", "Transformação: nulos, padronização e colunas derivadas.")
    aresta("alunos_transformado", "kpis_dashboard",      "Agregação de 11 KPIs executivos.")
    aresta("alunos_transformado", "analise_risco",       "Segmentação de risco de evasão por perfil.")
    aresta("alunos_transformado", "analise_engajamento", "Cálculo de índices de engajamento.")
    aresta("alunos_transformado", "insights",            "Geração de insights priorizados.")

    log.info("✅ Linhagem completa registrada.")


# ─── Task 3: Aplicar Governança ───────────────────────────────────────────────

def aplicar_governanca(**_):
    """
    Aplica owner, tags de classificação (PII / Tier) e descrições.

    Conceito: governança define quem é responsável pelos dados,
    qual sua sensibilidade (PII) e importância (Tier).
      - Tier1 = crítico para o negócio (tabelas Gold consumidas)
      - Tier3 = dado de suporte / externo (fonte e Bronze)
      - PII.Sensitive = contém dados pessoais identificáveis
    """
    from metadata.generated.schema.entity.data.table import Table
    from metadata.generated.schema.entity.teams.user import User
    from metadata.generated.schema.type.entityReference import EntityReference

    md = get_client()

    # (pii, tier, descrição)
    config = {
        "curso_txt":           (True,  "Tier.Tier3", "CSV fonte. Raiz do pipeline. Contém PII (NOME)."),
        "alunos_raw":          (True,  "Tier.Tier3", "Bronze: dados brutos, 199 registros. Contém PII."),
        "alunos_transformado": (True,  "Tier.Tier2", "Silver: dados tratados com colunas derivadas. Contém PII."),
        "kpis_dashboard":      (False, "Tier.Tier1", "Gold: 11 KPIs para dashboard executivo."),
        "analise_risco":       (False, "Tier.Tier1", "Gold: risco de evasão por perfil. 7 registros."),
        "analise_engajamento": (False, "Tier.Tier1", "Gold: índices de engajamento. 8 registros."),
        "insights":            (False, "Tier.Tier1", "Gold: insights priorizados. 6 registros."),
    }

    owner_user = md.get_by_name(entity=User, fqn="admin")
    owner_ref  = EntityReference(id=owner_user.id, type="user") if owner_user else None

    for nome, (pii, tier, desc) in config.items():
        tabela = md.get_by_name(entity=Table, fqn=f"{FQN_BASE}.{nome}")
        if not tabela:
            log.warning("⚠ Tabela não encontrada: %s", nome)
            continue
        try:
            md.patch_description(entity=Table, source=tabela, description=desc)
            md.patch_tag(entity=Table, source=tabela, tag_fqn=tier, is_suggested=False)
            if pii:
                md.patch_tag(entity=Table, source=tabela, tag_fqn="PII.Sensitive", is_suggested=False)
            if owner_ref:
                md.patch_owner(entity=Table, source=tabela, owner=owner_ref)
            log.info("✔ Governança: %s", nome)
        except Exception as e:
            log.warning("⚠ Erro em %s: %s", nome, e)

    log.info("✅ Governança aplicada em %d tabelas.", len(config))


# ─── Task 4: Registrar Glossário ─────────────────────────────────────────────

def registrar_glossario(**_):
    """
    Cria o Glossário de Educação e vincula termos às colunas Silver.

    Conceito: o glossário traduz termos técnicos para linguagem de negócio
    e vincula esses termos às colunas — facilitando a descoberta dos dados
    por analistas e gestores não-técnicos.
    """
    from metadata.generated.schema.api.data.createGlossary import CreateGlossaryRequest
    from metadata.generated.schema.api.data.createGlossaryTerm import CreateGlossaryTermRequest
    from metadata.generated.schema.entity.data.glossary import Glossary
    from metadata.generated.schema.entity.data.table import Table
    from metadata.generated.schema.type.entityReference import EntityReference

    md = get_client()
    GLOSSARIO = "Glossario_Educacao"

    glossario = md.get_by_name(entity=Glossary, fqn=GLOSSARIO)
    if not glossario:
        glossario = md.create_or_update(CreateGlossaryRequest(
            name=GLOSSARIO,
            displayName="Glossário Educação",
            description="Termos de negócio do domínio educacional.",
        ))

    ref = EntityReference(id=glossario.id, type="glossary")

    # Termos de negócio: (nome, displayName, descrição)
    termos = [
        ("MediaGeral",        "Média Geral",           "Média aritmética das notas nas 4 matérias."),
        ("TaxaPresenca",      "Taxa de Presença",      "Percentual de presença em relação às horas totais."),
        ("IndiceEngajamento", "Índice de Engajamento", "Índice composto: presença + conclusão de tarefas."),
        ("RiscoEvasao",       "Risco de Evasão",       "Risco de abandono baseado em reprovações e engajamento."),
        ("PerfilAluno",       "Perfil do Aluno",       "Segmentação comportamental/acadêmica do aluno."),
    ]
    for nome, display, desc in termos:
        try:
            md.create_or_update(CreateGlossaryTermRequest(
                glossary=ref, name=nome, displayName=display, description=desc,
            ))
            log.info("✔ Termo: %s", nome)
        except Exception as e:
            log.warning("⚠ Termo %s: %s", nome, e)

    # Vínculos coluna ↔ termo: (tabela, coluna, fqn_do_termo)
    vinculos = [
        ("alunos_transformado", "MEDIA_GERAL",        f"{GLOSSARIO}.MediaGeral"),
        ("alunos_transformado", "TAXA_PRESENCA",      f"{GLOSSARIO}.TaxaPresenca"),
        ("alunos_transformado", "INDICE_ENGAJAMENTO", f"{GLOSSARIO}.IndiceEngajamento"),
        ("alunos_transformado", "TOTAL_REPROVACOES",  f"{GLOSSARIO}.RiscoEvasao"),
        ("alunos_transformado", "PERFIL",             f"{GLOSSARIO}.PerfilAluno"),
        ("alunos_raw",          "PERFIL",             f"{GLOSSARIO}.PerfilAluno"),
    ]
    for tab, col, termo in vinculos:
        try:
            tabela = md.get_by_name(entity=Table, fqn=f"{FQN_BASE}.{tab}")
            if tabela:
                md.patch_column_tag(
                    entity=Table, source=tabela,
                    column_name=col, tag_fqn=termo, is_suggested=False,
                )
                log.info("✔ Vínculo: %s.%s → %s", tab, col, termo)
        except Exception as e:
            log.warning("⚠ Vínculo %s.%s: %s", tab, col, e)

    log.info("✅ Glossário registrado e vinculado.")


# ─── Task 5: Criar Domínio e Produto de Dados ─────────────────────────────────

def criar_dominio_produto(**_):
    """
    Cria o Domínio 'Educação' e o Produto de Dados com as tabelas Gold.

    Conceito: domínios organizam os assets por área de negócio.
    Produtos de Dados agrupam tabelas prontas para consumo analítico.
    """
    from metadata.generated.schema.api.domains.createDataProduct import CreateDataProductRequest
    from metadata.generated.schema.api.domains.createDomain import CreateDomainRequest
    from metadata.generated.schema.entity.domains.domain import Domain, DomainType

    md = get_client()

    try:
        dominio = md.get_by_name(entity=Domain, fqn="Educacao")
        if not dominio:
            dominio = md.create_or_update(CreateDomainRequest(
                name="Educacao",
                displayName="Educação",
                description="Domínio de dados educacionais — pipeline medallion de alunos.",
                domainType=DomainType.Consumer_aligned,
            ))
            log.info("✔ Domínio criado: Educacao")
    except Exception as e:
        log.warning("⚠ Erro ao criar domínio: %s", e)
        return

    try:
        md.create_or_update(CreateDataProductRequest(
            name="analytics-desempenho-alunos",
            displayName="Analytics de Desempenho de Alunos",
            description="KPIs, risco, engajamento e insights. Pipeline Bronze → Silver → Gold.",
            domain=dominio.fullyQualifiedName,
            assets=[
                f"{FQN_BASE}.kpis_dashboard",
                f"{FQN_BASE}.analise_risco",
                f"{FQN_BASE}.analise_engajamento",
                f"{FQN_BASE}.insights",
            ],
            experts=["admin"],
        ))
        log.info("✔ Produto de dados criado.")
    except Exception as e:
        log.warning("⚠ Erro ao criar produto de dados: %s", e)

    log.info("✅ Domínio e Produto de Dados registrados.")


# ─── Definição da DAG ─────────────────────────────────────────────────────────

with DAG(
    dag_id="7_catalog_open_metadata",
    description="Cataloga tabelas, linhagem, governança, glossário e domínios no OpenMetadata.",
    schedule=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["catalog", "governanca", "openmetadata"],
    default_args={
        "owner":  "engenharia.dados@empresa.com",
        "retries": 1,
    },
) as dag:

    t1 = PythonOperator(task_id="registrar_tabelas",     python_callable=registrar_tabelas)
    t2 = PythonOperator(task_id="registrar_lineage",     python_callable=registrar_lineage)
    t3 = PythonOperator(task_id="aplicar_governanca",    python_callable=aplicar_governanca)
    t4 = PythonOperator(task_id="registrar_glossario",   python_callable=registrar_glossario)
    t5 = PythonOperator(task_id="criar_dominio_produto", python_callable=criar_dominio_produto)

    t1 >> t2 >> t3 >> t4 >> t5
