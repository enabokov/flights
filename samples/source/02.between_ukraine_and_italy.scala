import org.apache.spark._
import org.apache.spark.graphx.{Graph, _}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SparkSession
import scala.collection.mutable.ListBuffer 

val spark = SparkSession.builder.getOrCreate()
val sqlContext = spark.sqlContext

case class Airport(name: String, country: String, visited: Boolean, path: String)

// Load data from CSV
val routes = sqlContext.read.format("csv").option("delimiter", ",").option("quote", "").option("header", "true").option("inferSchema", "true").csv("/opt/spark-data/routes.dat")
var airports = sqlContext.read.format("csv").option("delimiter", ",").option("quote", "").option("header", "true").option("inferSchema", "true").csv("/opt/spark-data/airports-extended.dat")

// select airports IDs, names and countries
val airportsVertices: RDD[(VertexId, Airport)] = airports.select(col("id").cast("long"), col("name"), col("country")).na.drop().distinct().rdd.map(row => (row.getLong(0), Airport(row.getString(1), row.getString(2), false, "")))
airportsVertices.count()
airportsVertices.take(10)

// select src_airport_id, dst_airport_id and as a property airline_id
val routesEdges: RDD[Edge[Long]] = routes.select(col("src_airport_id").cast("long"), col("dst_airport_id").cast("long"), col("stops").cast("int")).na.drop().rdd.map(row => Edge(row.getLong(0), row.getLong(1), row.getInt(2)))
routesEdges.count()

// verify data
routesEdges.take(10)

// build graph
val defaultRoute = Airport("unknown", "unknown", false, "")
val routeGraph = Graph(airportsVertices, routesEdges, defaultRoute)

// filter out all invalid routes
val validRouteGraph = routeGraph.subgraph(vpred = (id, airline) => airline.name != "unknown")
validRouteGraph.cache()

def getDFS[VT](graph: Graph[Airport,Long], sourceId: VertexId, distId: VertexId) = {
    val dfs = graph.pregel(Airport("\"Boryspil\"", "\"Ukraine\"", false, ""), 3, EdgeDirection.Out) (
      (vertex: VertexId, current: Airport, message: Airport) => {
        Airport(current.name, current.country, true, message.path) 
      },
      triplet => {
        val sourceVertex = triplet.srcAttr
        val destinationVertex = triplet.dstAttr

        if (destinationVertex.country == "\"Italy\"" && sourceVertex.country == "\"Ukraine\"") {
          // path += s"Direct ${sourceVertex.country} to ${destinationVertex.country} (${sourceVertex} to ${destinationVertex})"
          println(s"Direct flight ${sourceVertex.country} to ${destinationVertex.country} (${sourceVertex} to ${destinationVertex})")
          Iterator.empty
        } else if (destinationVertex.country == "\"Italy\""  && sourceVertex.country != "\"Italy\"") {
          println(s"${sourceVertex.path} via ${sourceVertex.country} to ${destinationVertex.country} (${sourceVertex.name} -- ${destinationVertex.name})")
          Iterator((triplet.dstId, Airport(destinationVertex.name, destinationVertex.country, true, s"${sourceVertex.path} via ${destinationVertex.country}")))
        } else if (sourceVertex.country == "\"Ukraine\"" && destinationVertex.country != "\"Ukraine\"") {
          println(s"Flight from ${sourceVertex.country} (${sourceVertex.name})")
          Iterator((triplet.dstId, Airport(destinationVertex.name, destinationVertex.country, true, s"from ${sourceVertex.country} (${sourceVertex.name})"))) 
        } else {
          Iterator.empty
        }
      },
      (msg1, msg2) => msg2
    ).cache()
    dfs
  }


getDFS(validRouteGraph, 2965, 2990)
