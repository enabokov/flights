#!/bin/bash -xe

. "${SPARK_HOME}"/sbin/spark-config.sh
. "${SPARK_HOME}"/bin/load-spark-env.sh

"${SPARK_HOME}"/sbin/../bin/spark-class \
    org.apache.spark.deploy.master.Master \
        --ip `hostname` \
        --port "${SPARK_MASTER_PORT}" \
        --webui-port "${SPARK_MASTER_WEBUI_PORT}"
