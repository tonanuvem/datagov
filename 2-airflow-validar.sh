#!/bin/bash

echo "=== Validação do Pipeline Airflow ==="
echo ""

# 1. Verificar se as DAGs estão ativas
echo "1. Verificando DAGs registradas..."
docker exec openmetadata_ingestion airflow dags list 2>/dev/null | grep -E "^[0-7]_"
echo ""

# 2. Executar a DAG orquestradora
echo "2. Executando DAG orquestradora..."
RUN_ID=$(docker exec openmetadata_ingestion airflow dags trigger 0_dag_orquestrador 2>/dev/null | grep -oP 'manual__\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d+\+\d{2}:\d{2}')
echo "Run ID: $RUN_ID"
echo ""

# 3. Monitorar execução via logs
echo "3. Monitorando execução do pipeline..."
MAX_WAIT=120
ELAPSED=0
while [ $ELAPSED -lt $MAX_WAIT ]; do
    STATE=$(docker exec openmetadata_ingestion airflow dags list-runs -d 0_dag_orquestrador --limit 1 2>/dev/null | tail -n 1 | awk '{print $4}')
    
    if [ "$STATE" = "success" ]; then
        echo "✅ Pipeline concluído com sucesso!"
        break
    elif [ "$STATE" = "failed" ]; then
        echo "❌ Pipeline falhou!"
        break
    else
        echo "⏳ Status: $STATE (${ELAPSED}s)"
        sleep 5
        ELAPSED=$((ELAPSED + 5))
    fi
done

if [ $ELAPSED -ge $MAX_WAIT ]; then
    echo "⚠️ Timeout: Pipeline ainda em execução após ${MAX_WAIT}s"
fi
echo ""

# 4. Exibir logs recentes
echo "4. Logs recentes do pipeline..."
docker logs openmetadata_ingestion 2>&1 | grep -E "(===|✅|❌|ERRO|CONCLUÍDO)" | tail -n 20
echo ""

# 5. Verificar arquivos gerados na camada Bronze
echo "5. Verificando arquivos na camada Bronze..."
ls -lh dados/bronze/
echo ""

# 6. Verificar arquivos gerados na camada Silver
echo "6. Verificando arquivos na camada Silver..."
ls -lh dados/silver/
echo ""

# 7. Verificar arquivos gerados na camada Gold
echo "7. Verificando arquivos na camada Gold..."
ls -lh dados/gold/
echo ""

# 8. Resumo final
echo "=== Resumo da Validação ==="
echo "Bronze: $(ls -1 dados/bronze/*.csv 2>/dev/null | wc -l) arquivo(s)"
echo "Silver: $(ls -1 dados/silver/*.csv 2>/dev/null | wc -l) arquivo(s)"
echo "Gold: $(ls -1 dados/gold/*.csv 2>/dev/null | wc -l) arquivo(s)"
echo ""
echo "Para ver logs detalhados de uma DAG específica:"
echo "docker logs openmetadata_ingestion 2>&1 | grep -A 20 'dag_id'"
