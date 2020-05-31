#!/bin/bash -xe

. "${SPARK_HOME}"/sbin/spark-config.sh
. "${SPARK_HOME}"/bin/load-spark-env.sh

"${SPARK_HOME}"/sbin/../bin/spark-class \
    org.apache.spark.deploy.worker.Worker \
        --webui-port "${SPARK_WORKER_WEBUI_PORT}" "${SPARK_MASTER}"
