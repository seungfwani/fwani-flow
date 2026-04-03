#!/bin/bash

## 도움말 출력하는 함수
usage() {
  echo "./build.sh [OPTIONS]"
  echo "    -h                도움말 출력"
  echo "    -v version        빌드 버전 (default: v0.1.0)"
  echo "    -o order          몇 번째 빌드인지 (default: 0)"
  echo "    -a                Airflow 이미지 빌드 포함 (default: false)"
  exit 0
}

if [ $# -eq 0 ];
then
  usage
  exit 0
fi

version=v0.1.0
order=0
build_airflow=false

while getopts "hv:o:a" opt
do
  case $opt in
    v) version=$OPTARG ;;
    o) order=$OPTARG ;;
    a) build_airflow=true ;;
    h) usage ;;
    ?) usage ;;
  esac
done

basePath=$(dirname $0)/..
cd $basePath
basePath=$(pwd)

today=$(date +%Y%m%d)

if [ "$build_airflow" = true ]; then
  echo "🔄 Build Airflow Image..."
  docker buildx build --platform=linux/amd64 \
      -t repo.iris.tools/graphio-dev/airflow:2.10.4-python3.11-${today}.${order} \
      -f docker/airflow.Dockerfile \
      $basePath
  docker push repo.iris.tools/graphio-dev/airflow:2.10.4-python3.11-${today}.${order}
  echo "✅ Complete build Airflow Image"
else
  echo "⏭️  Skip Airflow Image (use -a to build)"
fi

headHash=$(git rev-parse --short=7 HEAD)

echo "🔄 Build Workflow Server Image..."
docker buildx build --platform=linux/amd64 \
    -t repo.iris.tools/graphio-dev/workflow-server:${version}-${today}.${order}-${headHash} \
    -f docker/fwani-flow.Dockerfile \
    $basePath
docker push repo.iris.tools/graphio-dev/workflow-server:${version}-${today}.${order}-${headHash}
echo "✅ Complete build Workflow Server Image"
