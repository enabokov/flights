#!/bin/bash

set -e

docker build -t spark-base:2.4.5 -f docker/scala.Dockerfile ./docker
docker build -t spark-master:2.4.5 -f docker/spark-master.Dockerfile ./docker
docker build -t spark-worker:2.4.5 -f docker/spark-worker.Dockerfile ./docker
