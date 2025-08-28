#!/bin/bash

## 도움말 출력하는 함수
usage() {
  echo "./build.sh [OPTIONS]"
  echo "    -h                도움말 출력"
  echo "    -v version        빌드 버전 (default: v0.1.0)"
  echo "    -o order          몇 번째 빌드인지 (default: 0)"
  exit 0
}

if [ $# -eq 0 ];
then
  usage
  exit 0
fi

version=v0.1.0
order=0

while getopts "hv:o:" opt
do
  case $opt in
    v) version=$OPTARG ;;
    o) order=$OPTARG ;;
    h) usage ;;
    ?) usage ;;
  esac
done

basePath=$(dirname $0)/..
cd $basePath
basePath=$(pwd)

today=$(date +%Y%m%d)

echo "🔄 Build Airflow Image..."
docker buildx build --platform=linux/amd64 \
    -t repo.iris.tools/graphio/workflow/airflow:2.10.4-python3.11-${today}.${order} \
    -f docker/airflow.Dockerfile \
    $basePath
docker push repo.iris.tools/graphio/workflow/airflow:2.10.4-python3.11-${today}.${order}
echo "✅ Complete build Airflow Image"

headHash=$(git rev-parse --short=7 HEAD)

echo "🔄 Build Workflow Server Image..."
docker buildx build --platform=linux/amd64 \
    -t repo.iris.tools/graphio/workflow/workflow-server:${version}-${today}-${headHash} \
    -f docker/fwani-flow.Dockerfile \
    $basePath
docker push repo.iris.tools/graphio/workflow/workflow-server:${version}-${today}-${headHash}
echo "✅ Complete build Workflow Server Image"
