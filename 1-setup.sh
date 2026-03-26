#!/bin/bash

# Script de inicialização do ambiente
echo "=== Validando Estrutura do Projeto ==="
echo ""

# 1. Criar estrutura de diretórios
echo "Criando estrutura de diretórios..."
mkdir -p datapipeline/airflow/{dags,logs,plugins,config}
mkdir -p datapipeline/openmetadata/data
mkdir -p dados/{bronze,silver,gold}


# 2. Configurar permissões 
echo "Configurando permissões do OpenMetadata..."
chmod -R 777 datapipeline/openmetadata/data
echo "Configurando permissões do Airflow..."
chmod -R 777 datapipeline/airflow/{logs,plugins}


# 3. Inicializar banco de dados do OpenMetadata
echo "Inicializando PostGres..."
# Não precisa subir separado, o postgres.yml já inclui tudo

# 4. Subir todos os serviços : Open Metadata e Ingestion Airflow
echo "Subindo serviços..."
docker-compose -f datapipeline/openmetadata/docker-compose-postgres.yml up -d


# 5. Aguardar serviços ficarem prontos
echo ""
echo "Aguardando serviços ficarem prontos..."
echo -n "Airflow"
while ! docker logs openmetadata_ingestion 2>&1 | grep -q "Uvicorn running on"; do
    echo -n "."
    sleep 2
done
echo " ✅"

echo -n "OpenMetadata"
while ! docker logs openmetadata_server 2>&1 | grep -q "Started oejs.Server"; do
    echo -n "."
    sleep 2
done
echo " ✅"

# 6. Verificar status dos serviços
echo "Verificando status dos serviços..."
docker ps --format "table {{.Names}}\t{{.Status}}\t{{.Ports}}"

IP=$(curl https://checkip.amazonaws.com)
echo ""
echo "=== Ambiente inicializado com sucesso! ==="
echo ""
echo "Acesse os serviços:"
echo "- Airflow: http://$IP:8080 (usuário: admin, senha: admin)"
echo "- OpenMetadata: http://$IP:8585 (usuário: admin@open-metadata.org, senha: admin)"
echo ""
echo ""
echo "Acesse o Airflow e veja se aparecem as DAGs na ordem:"
echo "0. dag_orquestrador"
echo "1. bronze_ingestion"
echo "2. bronze_test"
echo "3. silver_transformation"
echo "4. silver_test"
echo "5. gold_aggregation"
echo "6. gold_test"
echo "7. catalog_metadata"
