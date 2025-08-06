#!/bin/bash

basePath=$(dirname $0)/..
cd $basePath
basePath=$(pwd)

echo "🔄 Build Airflow Image..."
docker buildx build --platform=linux/amd64 \
    -t repo.iris.tools/graphio/workflow/airflow:2.10.4-python3.11 \
    -f docker/airflow.Dockerfile \
    $basePath
docker push repo.iris.tools/graphio/workflow/airflow:2.10.4-python3.11
echo "✅ Complete build Airflow Image"

today=$(date +%Y%m%d)
headHash=$(git rev-parse --short=7 HEAD)

echo "🔄 Build Workflow Server Image..."
docker buildx build --platform=linux/amd64 \
    -t repo.iris.tools/graphio/workflow/workflow-server:v0.1.3-${today}-${headHash} \
    -f docker/fwani-flow.Dockerfile \
    $basePath
docker push repo.iris.tools/graphio/workflow/workflow-server:v0.1.3-${today}-${headHash}
echo "✅ Complete build Workflow Server Image"

