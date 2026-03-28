"""
DAG de Catalogação no OpenMetadata — Versão Didática Simplificada
=================================================================
Demonstra os conceitos principais de governança de dados:

  1. buscar_token_bot        → login admin → pega JWT do IngestionBot via API
  2. registrar_tabelas       → cadastra as tabelas do pipeline medallion
  3. registrar_lineage       → curso_txt → bronze → silver → gold
  4. aplicar_governanca      → owner, tags PII/Tier e descrições
  5. registrar_glossario     → termos de negócio (Glossário Educação)
  6. criar_dominio_produto   → Domínio Educação + Produto de Dados

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
AUTENTICAÇÃO — Como configurar a Airflow Connection:

  Airflow UI → Admin → Connections → [+] Add a new record
    Connection Id : openmetadata_default
    Connection Type: HTTP
    Host          : openmetadata-server
    Port          : 8585
    Login         : admin@open-metadata.org
    Password      : admin

  Ou via CLI:
    airflow connections add openmetadata_default \
        --conn-type http \
        --conn-host openmetadata-server \
        --conn-port 8585 \
        --conn-login "admin@open-metadata.org" \
        --conn-password "admin"

  Fluxo de autenticação:
    1. t0 faz POST /api/v1/users/login com login+senha da Connection
    2. Recebe token temporário de admin
    3. Usa esse token para GET /api/v1/bots/name/ingestion-bot
    4. Busca o JWT permanente do IngestionBot
    5. Publica no XCom → todas as tasks seguintes usam esse JWT
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

import requests
from airflow import DAG
from airflow.hooks.base import BaseHook
from airflow.operators.python import PythonOperator

# ─── Configuração ─────────────────────────────────────────────────────────────

OM_CONN_ID = "openmetadata_default"
SERVICE    = "pipeline_alunos"
DATABASE   = "educacao"
SCHEMA     = "camadas"
FQN_BASE   = f"{SERVICE}.{DATABASE}.{SCHEMA}"

log = logging.getLogger(__name__)


# ─── Helper: lê host e credenciais da Airflow Connection ──────────────────────

def get_om_base_url() -> str:
    conn = BaseHook.get_connection(OM_CONN_ID)
    return f"http://{conn.host}:{conn.port}/api"


def get_om_credentials() -> tuple[str, str]:
    """Retorna (email, senha) da Airflow Connection."""
    conn = BaseHook.get_connection(OM_CONN_ID)
    if not conn.login or not conn.password:
        raise ValueError(
            f"Login ou senha não encontrados na Connection '{OM_CONN_ID}'. "
            "Configure os campos Login e Password."
        )
    return conn.login, conn.password


# ─── Helper: cliente autenticado com token do XCom ────────────────────────────

def get_client(ti=None):
    """
    Retorna cliente OpenMetadata autenticado.
    Prioriza o JWT do IngestionBot via XCom (publicado pela t0).
    """
    from metadata.generated.schema.entity.services.connections.metadata.openMetadataConnection import (
        OpenMetadataConnection,
    )
    from metadata.generated.schema.security.client.openMetadataJWTClientConfig import (
        OpenMetadataJWTClientConfig,
    )
    from metadata.ingestion.ometa.ometa_api import OpenMetadata

    base_url = get_om_base_url()

    if ti:
        token = ti.xcom_pull(task_ids="buscar_token_bot", key="bot_jwt_token")
        if not token:
            raise ValueError(
                "Token do IngestionBot não encontrado no XCom. "
                "Verifique se a task 'buscar_token_bot' executou com sucesso."
            )
    else:
        raise ValueError("TaskInstance (ti) não fornecido — impossível obter token do XCom.")

    return OpenMetadata(OpenMetadataConnection(
        hostPort=base_url,
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


# ─── Task 0: Buscar Token do IngestionBot ─────────────────────────────────────

def buscar_token_bot(**context):
    """
    Autentica como admin via usuário/senha e busca o JWT do IngestionBot.

    Passo a passo:
      1. POST /api/v1/users/login  → token temporário de admin
      2. GET  /api/v1/bots/name/ingestion-bot → dados do bot (botUser.id)
      3. GET  /api/v1/users/{id}/token        → JWT permanente do bot
      4. XCom push → tasks seguintes consomem via xcom_pull
    """
    base_url = get_om_base_url()
    email, senha = get_om_credentials()

    # ── 1. Login com usuário/senha → token temporário ─────────────────────────
    log.info("🔐 Autenticando admin no OpenMetadata...")
    resp_login = requests.post(
        f"{base_url}/v1/users/login",
        json={"email": email, "password": senha},
        timeout=30,
    )
    resp_login.raise_for_status()
    admin_token = resp_login.json()["accessToken"]
    log.info("✔ Login admin bem-sucedido.")

    headers = {
        "Authorization": f"Bearer {admin_token}",
        "Content-Type": "application/json",
    }

    # ── 2. Busca dados do IngestionBot ────────────────────────────────────────
    log.info("🤖 Buscando dados do IngestionBot...")
    resp_bot = requests.get(
        f"{base_url}/v1/bots/name/ingestion-bot",
        headers=headers,
        timeout=30,
    )
    resp_bot.raise_for_status()
    bot_data = resp_bot.json()

    bot_user_id = bot_data["botUser"]["id"]
    log.info("✔ IngestionBot user ID: %s", bot_user_id)

    # ── 3. Busca JWT permanente do bot ────────────────────────────────────────
    log.info("🔑 Buscando JWT do IngestionBot...")
    resp_token = requests.get(
        f"{base_url}/v1/users/{bot_user_id}/token",
        headers=headers,
        timeout=30,
    )
    resp_token.raise_for_status()
    token_data = resp_token.json()

    jwt_token = token_data["JWTToken"]
    log.info("✔ JWT do IngestionBot obtido com sucesso.")

    # ── 4. Publica no XCom ────────────────────────────────────────────────────
    context["ti"].xcom_push(key="bot_jwt_token", value=jwt_token)
    log.info("✅ Token publicado no XCom.")
    return jwt_token


# ─── Task 1: Registrar Tabelas ────────────────────────────────────────────────

def registrar_tabelas(ti=None, **_):
    """
    Registra todas as tabelas do pipeline no catálogo.
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

    md = get_client(ti=ti)

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

        # FONTE
        CreateTableRequest(
            name="curso_txt",
            displayName="Fonte: curso.txt (CSV)",
            description="Arquivo CSV de origem. Raiz do grafo: curso_txt → Bronze → Silver → Gold.",
            tableType=TableType.Regular,
            databaseSchema=FQN_BASE,
            columns=_colunas_base(),
        ),

        # BRONZE
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

        # SILVER
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

        # GOLD
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

def registrar_lineage(ti=None, **_):
    """
    Registra o grafo de linhagem completo.
    """
    from metadata.generated.schema.api.lineage.addLineage import AddLineageRequest
    from metadata.generated.schema.entity.data.table import Table
    from metadata.generated.schema.type.entityLineage import EntitiesEdge, LineageDetails
    from metadata.generated.schema.type.entityReference import EntityReference

    md = get_client(ti=ti)

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

def aplicar_governanca(ti=None, **_):
    """
    Aplica owner, tags de classificação (PII / Tier) e descrições.
    """
    from metadata.generated.schema.entity.data.table import Table
    from metadata.generated.schema.entity.teams.user import User
    from metadata.generated.schema.type.entityReference import EntityReference

    md = get_client(ti=ti)

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

def registrar_glossario(ti=None, **_):
    """
    Cria o Glossário de Educação e vincula termos às colunas Silver.
    """
    from metadata.generated.schema.api.data.createGlossary import CreateGlossaryRequest
    from metadata.generated.schema.api.data.createGlossaryTerm import CreateGlossaryTermRequest
    from metadata.generated.schema.entity.data.glossary import Glossary
    from metadata.generated.schema.entity.data.table import Table
    from metadata.generated.schema.type.entityReference import EntityReference

    md = get_client(ti=ti)
    GLOSSARIO = "Glossario_Educacao"

    glossario = md.get_by_name(entity=Glossary, fqn=GLOSSARIO)
    if not glossario:
        glossario = md.create_or_update(CreateGlossaryRequest(
            name=GLOSSARIO,
            displayName="Glossário Educação",
            description="Termos de negócio do domínio educacional.",
        ))

    ref = EntityReference(id=glossario.id, type="glossary")

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

def criar_dominio_produto(ti=None, **_):
    """
    Cria o Domínio 'Educação' e o Produto de Dados com as tabelas Gold.
    """
    from metadata.generated.schema.api.domains.createDataProduct import CreateDataProductRequest
    from metadata.generated.schema.api.domains.createDomain import CreateDomainRequest
    from metadata.generated.schema.entity.domains.domain import Domain, DomainType

    md = get_client(ti=ti)

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

    t0 = PythonOperator(task_id="buscar_token_bot",      python_callable=buscar_token_bot)
    t1 = PythonOperator(task_id="registrar_tabelas",     python_callable=registrar_tabelas)
    t2 = PythonOperator(task_id="registrar_lineage",     python_callable=registrar_lineage)
    t3 = PythonOperator(task_id="aplicar_governanca",    python_callable=aplicar_governanca)
    t4 = PythonOperator(task_id="registrar_glossario",   python_callable=registrar_glossario)
    t5 = PythonOperator(task_id="criar_dominio_produto", python_callable=criar_dominio_produto)

    t0 >> t1 >> t2 >> t3 >> t4 >> t5
