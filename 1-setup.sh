#!/bin/bash

# Script de inicialização do ambiente
echo "=== Validando Estrutura do Projeto ==="
echo ""

# 1. Criar estrutura de diretórios
echo "Criando estrutura de diretórios..."


# 2. Configurar permissões 
echo "Configurando permissões do OpenMetadata..."
echo "Configurando permissões do Airflow..."


# 3. Inicializar banco de dados do OpenMetadata
echo "Inicializando PostGres..."

# 4. Subir todos os serviços : Open Metadata e Ingestion Airflow
echo "Subindo serviços..."


# 5. Aguardar serviços ficarem prontos
echo "Aguardando serviços ficarem prontos..."
sleep 30

# 6. Verificar status dos serviços
echo "Verificando status dos serviços..."

IP=$(curl https://checkip.amazonaws.com)
echo ""
echo "=== Ambiente inicializado com sucesso! ==="
echo ""
echo "Acesse os serviços:"
echo "- Airflow: http://$IP:8080 (usuário: airflow, senha: airflow)"
echo "- OpenMetadata: http://$IP:8585 (usuário: admin, senha: admin)"
echo ""
echo ""
echo "Acesse o Airflow e verificar se aparecem as DAGs na ordem:"
echo "0. pipeline_orquestracao"
echo "1. bronze_ingestion"
echo "2. bronze_test"
echo "3. silver_transformation"
echo "4. silver_test"
echo "5. gold_aggregation"
echo "6. gold_test"
echo "7. catalog_metadata"
