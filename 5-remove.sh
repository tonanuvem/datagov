#!/bin/bash

# Lista todos os yml e baixa cada um
ls datapipeline/openmetadata/*.yml | while read yml; do
  echo "Removendo projeto: $yml"
  docker-compose -f $yml stop && docker-compose -f $yml rm -f
done
echo ""
docker volume prune -f && git pull

echo ""
echo "Removendo volumes:"
echo ""
# Lista todos os volumes e remove cada um
docker volume ls -q | grep relatoriosbi | while read volume; do
  #echo "Removendo volume: $volume"
  docker volume rm "$volume"
done
echo ""

#docker network rm app_net
docker network rm datacatalog_app_net
