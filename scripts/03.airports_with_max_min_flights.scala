import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SparkSession

val spark = SparkSession.builder.getOrCreate()
val sqlContext = spark.sqlContext

case class Airport(name: String, country: String)

// Load data from CSV
val routes = sqlContext.read.format("csv").option("delimiter", ",").option("quote", "").option("header", "true").option("inferSchema", "true").csv("/opt/spark-data/routes.dat")
var airports = sqlContext.read.format("csv").option("delimiter", ",").option("quote", "").option("header", "true").option("inferSchema", "true").csv("/opt/spark-data/airports-extended.dat")

// select airports IDs, names and countries
val airportsVertices: RDD[(VertexId, Airport)] = airports.select(col("id").cast("long"), col("name"), col("country")).na.drop().distinct().rdd.map(row => (row.getLong(0), Airport(row.getString(1), row.getString(2))))
airportsVertices.count()
airportsVertices.take(10)

// select src_airport_id, dst_airport_id and as a property stops
val routesEdges: RDD[Edge[Long]] = routes.select(col("src_airport_id").cast("long"), col("dst_airport_id").cast("long"), col("stops").cast("int")).na.drop().rdd.map(row => Edge(row.getLong(0), row.getLong(1), row.getInt(2)))
routesEdges.count()

// verify data
routesEdges.take(10)

// build graph
val defaultRoute = Airport("unknown", "unknown")
val routeGraph = Graph(airportsVertices, routesEdges, defaultRoute)

// filter out all invalid routes
val validRouteGraph = routeGraph.subgraph(vpred = (id, airport) => airport.name != "unknown")
validRouteGraph.cache()

// query 3

// airport with max sum of distances
def max(a: (VertexId, Int), b: (VertexId, Int)): (VertexId, Int) = {
  if (a._2 > b._2) a else b
}

val maxDegrees: (Long, Int) = validRouteGraph.degrees.reduce(max)
val airportWithMaxFlights = validRouteGraph.vertices.filter(x => x._1 == maxDegrees._1).collect
airportWithMaxFlights.foreach(println)

// airport min sum of distances
def min(a: (VertexId, Int), b: (VertexId, Int)): (VertexId, Int) = {
  if (a._2 < b._2) a else b
}
val minDegrees: (Long, Int) = validRouteGraph.degrees.reduce(min)
val airportWithMinFlights = validRouteGraph.vertices.filter(x => x._1 == minDegrees._1).collect
airportWithMinFlights.foreach(println)
