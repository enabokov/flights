import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SparkSession
import scala.math._

val spark = SparkSession.builder.getOrCreate()
val sqlContext = spark.sqlContext

case class Airport(name: String, country: String, latitude: Double, longitude: Double)

// Load data from CSV
val routes = sqlContext.read.format("csv").option("delimiter", ",").option("quote", "").option("header", "true").option("inferSchema", "true").csv("/opt/spark-data/routes.dat")
var airports = sqlContext.read.format("csv").option("delimiter", ",").option("quote", "").option("header", "true").option("inferSchema", "true").csv("/opt/spark-data/airports-extended.dat")

// select airports IDs, names and countries
val airportsVertices: RDD[(VertexId, Airport)] = airports.select(col("id").cast("long"), col("name"), col("country"), col("latitude").cast("double"), col("longitude").cast("double")).na.drop().distinct().rdd.map(row => (row.getLong(0), Airport(row.getString(1), row.getString(2), row.getDouble(3), row.getDouble(4))))
airportsVertices.count()
airportsVertices.take(10)

// select src_airport_id, dst_airport_id and as a property stops
val routesEdges: RDD[Edge[Long]] = routes.select(col("src_airport_id").cast("long"), col("dst_airport_id").cast("long"), col("stops").cast("int")).na.drop().rdd.map(row => Edge(row.getLong(0), row.getLong(1), row.getInt(2)))
routesEdges.count()

// verify data
routesEdges.take(10)

// build graph
val defaultRoute = Airport("unknown", "unknown", -1, -1)
val routeGraph = Graph(airportsVertices, routesEdges, defaultRoute)

// filter out all invalid routes
val validRouteGraph = routeGraph.subgraph(vpred = (id, airport) => airport.name != "unknown")
validRouteGraph.cache()

// query 4

def calc_distance(lat1: Double, lon1: Double, lat2: Double, lon2: Double): Double = {
  // distance between latitudes and longitudes
  val dLat = (lat2 - lat1) * Pi / 180.0
  val dLon = (lon2 - lon1) * Pi / 180.0

  // convert to radian
  val radLat1 = (lat1) * Pi / 180.0
  val radLat2 = (lat2) * Pi / 180.0

  // magic
  val a = (pow(sin(dLat / 2), 2) + 
         pow(sin(dLon / 2), 2) * 
             cos(radLat1) * cos(radLat2)); 
  val rad = 6371
  val c = 2 * asin(sqrt(a)) 
  return rad * c 
}


val desiredSrcAirportName = "\"Churchill Airport\""
val desiredDstAirportName = "\"London Heathrow Airport\""

// val srcAirport = airportsVertices.filter(airport => airport._2.name == desiredSrcAirportName).take(1)(0)
// val dstAirport = airportsVertices.filter(airport => airport._2.name == desiredDstAirportName).take(1)(0)

val srcAirportVertex = validRouteGraph.vertices.filter(x => x._2.name == desiredSrcAirportName).take(1)(0)
val dstAirportVertex = validRouteGraph.vertices.filter(x => x._2.name == desiredDstAirportName).take(1)(0)

// validRouteGraph.edges.filter{ case Edge(srcId, dstId, prop) => (srcId == srcAirport._1 && dstId == dstAirport._1)}.collect.mkString("\n")
// validRouteGraph.collectEdges(EdgeDirection.Out).collect.

// validRouteGraph.collectEdges(EdgeDirection.Out).filter {case (id, edges) => { val cedge = Edge(srcId, dstId, prop) for (edge <- edges if (edge.srcId == )) yield cedge }  (srcId == srcAirport._1 && dstId == dstAirport._1)

// validRouteGraph.collectNeighbors(EdgeDirection.In).take(1) foreach { case (id, neighboars) => {val airport: Airport = for (nei <- neighboars) yield nei._1 println(airport)}}

val newGraph = validRouteGraph.mapEdges(
  edge => (
    calc_distance(
      validRouteGraph.vertices.filter(x => edge.srcId == x._1).take(1)(0)._2.latitude,
      validRouteGraph.vertices.filter(x => edge.srcId == x._1).take(1)(0)._2.longitude,
      validRouteGraph.vertices.filter(x => edge.dstId == x._2).take(1)(0)._2.latitude,
      validRouteGraph.vertices.filter(x => edge.dstId == x._2).take(1)(0)._2.longitude
    )
  )
)

newGraph.edges.take(5)