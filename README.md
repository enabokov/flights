# Analysis of flights with GraphX

Implemeted with spark and scala using graphx

## HowTo

### Start 4 containers with spark (1 master and 3 workers).

```
docker-compose -f docker-compose.yml up --scale spark-worker=3
```

### Login into spark master

```
docker exec -it spark-master bash
```

### Login into Spark shell

```
/spark/bin/spark-shell
```

### Execute scala scripts

```
:load /opt/spark-scripts/01.max_min_distance.scala
```

## Access traefik dashboard

```
http://localhost:8080/dashboard/
````

## Access spark master web ui

```
http://sparkweb.docker.localhost/
```
