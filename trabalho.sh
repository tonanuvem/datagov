echo "Baixando a solução: "
echo ""
echo "Baixando a solução: 1/4"
echo ""
docker pull docker.getcollate.io/openmetadata/postgresql:1.12.0
echo ""
echo "Baixando a solução: 2/4"
echo ""
docker pull docker.elastic.co/elasticsearch/elasticsearch:9.3.0
echo ""
echo "Baixando a solução: 3/4"
echo ""
docker pull docker.getcollate.io/openmetadata/server:1.12.0
echo ""
echo "Baixando a solução: 4/4"
echo ""
docker pull docker.getcollate.io/openmetadata/ingestion:1.12.3

echo "Executando a solução:"
sh 1-setup.sh