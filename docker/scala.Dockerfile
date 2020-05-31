FROM java:8-jdk-alpine

ENV SPARK_VERSION=2.4.5
ENV SPARK_HOME=/opt/spark
ENV SBT_HOME=/opt/sbt
ENV HADOOP_VERSION=2.7
ENV SCALA_VERSION=2.12.4
# check sbt versions here: https://www.scala-sbt.org/download.html
ENV SBT_VERSION=sbt-1.3.10

RUN apk update && apk add wget bash tar

# install sbt (java & scala)
WORKDIR ${SBT_HOME}
ADD https://piccolo.link/${SBT_VERSION}.tgz https://piccolo.link/${SBT_VERSION}.tgz.sha256 ./
RUN sha256sum ${SBT_VERSION}.tgz > actual_sha \
    && diff actual_sha ${SBT_VERSION}.tgz.sha256 || exit 1 \
    && tar -xf ${SBT_VERSION}.tgz \
    && rm ${SBT_VERSION}.tgz ${SBT_VERSION}.tgz.sha256 actual_sha

# install sbt
RUN ${SBT_HOME}/sbt/bin/sbt sbtVersion \
    && ln -s ${SBT_HOME}/sbt/bin/sbt /usr/local/bin/sbt

# install spark
WORKDIR ${SPARK_HOME}
ADD http://apache.mirror.iphh.net/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz .
RUN tar -xvzf spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz \
    && mv spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}/* . \
    && rm spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz

WORKDIR /home
