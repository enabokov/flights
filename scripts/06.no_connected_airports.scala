import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SparkSession

val spark = SparkSession.builder.getOrCreate()
val sqlContext = spark.sqlContext

// Load data from CSV
val routes = sqlContext.read.format("csv").option("delimiter", ",").option("quote", "").option("header", "true").option("inferSchema", "true").csv("/opt/spark-data/routes.dat")
var airlines = sqlContext.read.format("csv").option("delimiter", ",").option("quote", "").option("header", "true").option("inferSchema", "true").csv("/opt/spark-data/airlines.dat")

// select airline ID and its name
val airlinesVertices: RDD[(VertexId, String)] = airlines.select(col("airline_id").cast("long"), col("name")).na.drop().distinct().rdd.map(row => (row.getLong(0), row.getString(1)))
airlinesVertices.count()
airlinesVertices.take(10)

// select src_airport_id, dst_airport_id and as a property airline_id
val routesEdges: RDD[Edge[Long]] = routes.select(col("src_airport_id").cast("long"), col("dst_airport_id").cast("long"), col("airline_id").cast("long")).na.drop().rdd.map(row => Edge(row.getLong(0), row.getLong(1), row.getLong(2)))
routesEdges.count()

// verify data
routesEdges.take(10)

// build graph
val defaultRoute = ("unknown")
val routeGraph = Graph(airlinesVertices, routesEdges, defaultRoute)

// filter out all invalid routes
val validRouteGraph = routeGraph.subgraph(vpred = (id, name) => name != "unknown")
validRouteGraph.cache()

// // search and exclude
val connectedVertices = validRouteGraph.connectedComponents().vertices.collect
validRouteGraph.subgraph(vpred = (id, attr) => !connectedVertices.contains(id)).vertices.collect.take(10).foreach(println)
