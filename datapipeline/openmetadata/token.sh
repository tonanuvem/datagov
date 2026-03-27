#!/bin/bash
# Script para obter e configurar o token JWT do ingestion-bot no OpenMetadata

set -e

echo "=== Configuração de Token OpenMetadata ==="
echo ""

# 1. Aguardar OpenMetadata estar pronto
echo "Aguardando OpenMetadata iniciar..."
sleep 5

# 2. Fazer login como admin e obter token de acesso
echo "Autenticando como admin..."
ADMIN_TOKEN=$(docker exec openmetadata_ingestion curl -s -X POST \
  http://openmetadata-server:8585/api/v1/users/login \
  -H "Content-Type: application/json" \
  -d '{"email":"admin@open-metadata.org","password":"YWRtaW4="}' | \
  python3 -c "import sys,json; print(json.load(sys.stdin)['accessToken'])" 2>/dev/null)

if [ -z "$ADMIN_TOKEN" ]; then
    echo "❌ Falha ao autenticar como admin"
    exit 1
fi

echo "✅ Autenticado com sucesso"

# 3. Obter token do ingestion-bot
echo "Obtendo token do ingestion-bot..."
NEW_TOKEN=$(docker exec openmetadata_ingestion curl -s \
  http://openmetadata-server:8585/api/v1/users/name/ingestion-bot \
  -H "Authorization: Bearer $ADMIN_TOKEN" | \
  python3 -c "import sys,json; d=json.load(sys.stdin); print(d['authenticationMechanism']['config']['JWTToken'])" 2>/dev/null)

if [ -z "$NEW_TOKEN" ]; then
    echo "❌ Falha ao obter token do bot"
    exit 1
fi

echo "✅ Token obtido com sucesso"
echo ""

# 4. Atualizar docker-compose com o novo token
echo "Atualizando docker-compose-postgres.yml..."
cd /home/ec2-user/datagov/datapipeline/openmetadata

# Fazer backup
cp docker-compose-postgres.yml docker-compose-postgres.yml.bak

# Substituir tokens no arquivo
sed -i "s|AIRFLOW__LINEAGE__JWT_TOKEN: \".*\"|AIRFLOW__LINEAGE__JWT_TOKEN: \"$NEW_TOKEN\"|g" docker-compose-postgres.yml
sed -i "s|OPENMETADATA_JWT_TOKEN: \".*\"|OPENMETADATA_JWT_TOKEN: \"$NEW_TOKEN\"|g" docker-compose-postgres.yml

echo "✅ Arquivo docker-compose atualizado"
echo ""

# 5. Reiniciar container Airflow para aplicar mudanças
echo "Reiniciando container Airflow..."
docker-compose -f docker-compose-postgres.yml restart ingestion
sleep 15

echo "✅ Container reiniciado"
echo ""

# 6. Validar configuração
echo "=== Validação da Configuração ==="
echo ""

# Verificar se o token está no container
CONFIGURED_TOKEN=$(docker exec openmetadata_ingestion bash -c 'echo $AIRFLOW__LINEAGE__JWT_TOKEN' 2>/dev/null || echo "")

if [ -n "$CONFIGURED_TOKEN" ] && [ "$CONFIGURED_TOKEN" == "$NEW_TOKEN" ]; then
    echo "✅ Token configurado corretamente no container"
else
    echo "⚠️  Token no container: ${CONFIGURED_TOKEN:0:50}..."
fi

# Testar conexão com OpenMetadata
echo ""
echo "Testando conexão com OpenMetadata..."
TEST_RESULT=$(docker exec openmetadata_ingestion curl -s -o /dev/null -w "%{http_code}" \
  http://openmetadata-server:8585/api/v1/system/version \
  -H "Authorization: Bearer $NEW_TOKEN")

if [ "$TEST_RESULT" == "200" ]; then
    echo "✅ Conexão com OpenMetadata validada com sucesso"
else
    echo "⚠️  Código HTTP: $TEST_RESULT"
fi

echo ""
echo "=== Configuração Concluída ==="
echo ""
echo "Token configurado (primeiros 50 caracteres):"
echo "${NEW_TOKEN:0:50}..."
echo ""
echo "Para usar em DAGs, o token está disponível em:"
echo "  - Variável de ambiente: AIRFLOW__LINEAGE__JWT_TOKEN"
echo "  - Variável de ambiente: OPENMETADATA_JWT_TOKEN"
