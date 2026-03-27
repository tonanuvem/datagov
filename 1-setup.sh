#!/bin/bash

# Script de inicialização do ambiente
echo "=== Validando Estrutura do Projeto ==="
echo ""

# 1. Criar estrutura de diretórios
echo "Criando estrutura de diretórios..."
mkdir -p datapipeline/airflow/{dags,logs,plugins,config}
mkdir -p datapipeline/openmetadata/docker-volume/db-data-postgres
mkdir -p dados/{bronze,silver,gold}


# 2. Configurar permissões 
echo "Configurando permissões do PostgreSQL..."
sudo chown -R 999:999 datapipeline/openmetadata/docker-volume/db-data-postgres
echo "Configurando permissões do Airflow..."
chmod -R 777 datapipeline/airflow/{logs,plugins}
echo "Configurando permissões do diretório de dados (UID 50000 = Airflow container)..."
sudo chown -R 50000:0 dados/
sudo chmod -R 755 dados/


# 3. Inicializar banco de dados do OpenMetadata
echo "Verificando containers existentes..."
docker ps -a | grep -q openmetadata_postgresql && docker rm -f openmetadata_postgresql 2>/dev/null || true
docker ps -a | grep -q openmetadata_elasticsearch && docker rm -f openmetadata_elasticsearch 2>/dev/null || true
docker ps -a | grep -q openmetadata_server && docker rm -f openmetadata_server 2>/dev/null || true
docker ps -a | grep -q openmetadata_ingestion && docker rm -f openmetadata_ingestion 2>/dev/null || true
docker ps -a | grep -q execute_migrate_all && docker rm -f execute_migrate_all 2>/dev/null || true

# 4. Subir todos os serviços : Open Metadata e Ingestion Airflow
echo "Subindo serviços..."
echo "Aguardando PostgreSQL inicializar..."
docker-compose -f datapipeline/openmetadata/docker-compose-postgres.yml up -d postgresql
sleep 10

echo "Subindo demais serviços..."
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

# 6. Configurar AirFlow : 8 slots separados, um para cada task das DAGs para usar slots independentes e nao travar
echo "Configurando AiirFlow..."
# Pegar o token JWT para autenticar na API v2
TOKEN=$(curl -s -X POST http://localhost:8080/auth/token \
  -H "Content-Type: application/json" \
  -d '{"username": "admin", "password": "admin"}' | python3 -c "import sys,json; print(json.load(sys.stdin)['access_token'])")

#echo "Token: $TOKEN"

# Criar o pool com o token
curl -X POST http://localhost:8080/api/v2/pools \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer $TOKEN" \
  -d '{"name": "pipeline_pool", "slots": 8, "description": "Pool pipeline alunos"}'

# 7. Configurar token do OpenMetadata
echo ""
echo "Configurando token do OpenMetadata..."
bash datapipeline/openmetadata/token.sh

# 8. Verificar status dos serviços
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
echo "Acesse o Airflow e veja se aparecem as DAGs"
