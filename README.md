# Analysis of flights with GraphX

Implemented with Spark and Scala using GraphX

## HowTo

1. Start 4 containers with spark (1 master and 3 workers).

```
docker-compose -f docker-compose.yml up --scale spark-worker=3
```

2. Login into a container with Spark master

```
docker exec -it flights-spark-master bash
```

3. Login into Spark shell

```
/spark/bin/spark-shell
```

4. Execute scala scripts

```
:load /opt/spark-scripts/01.max_min_distance.scala
```

## Register application

```
/opt/spark/bin/spark-submit \
    --class org.apache.spark.examples.SparkPi \
    --master spark://<spark-master-container-id>:7077 \
    --deploy-mode cluster \
    --supervise \
    --executor-memory 3G \
    --total-executor-cores 10 \
    /opt/spark/examples/jars/spark-examples_2.11-2.4.5.jar 100
```

## Monitoring

### Access traefik dashboard

```
http://localhost:8080/dashboard/
````

### Access spark master web ui

```
http://sparkweb.docker.localhost/
```

> Note: to make the fqdn resolvable, add 127.0.0.1 *.docker.localhost to /etc/hosts

or

```
curl -H Host:sparkweb.docker.localhost localhost
```
