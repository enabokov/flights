FROM spark-base:2.4.5

COPY start-master.sh /

ENV SPARK_MASTER_PORT 7077
ENV SPARK_MASTER_WEBUI_PORT 8080

EXPOSE 8080 7077 6066

CMD ["/bin/bash", "/start-master.sh"]
