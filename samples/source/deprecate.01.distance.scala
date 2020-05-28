import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SparkSession

// case class Airport(
//     id: Int,
//     name: String,
//     city: String,
//     country: String,
//     iata: String,
//     icao: String,
//     latitude: Double,
//     longitude: Double,
//     altitude: Int,
//     timezone: Int,
//     dst: Char,
//     tz: String,
//     ztype: String,
//     source: String
// )


// val iter = src.getLines().map(_.split(","))

// val filename = "/opt/spark-data/airports-extended.dat"
// for (line <- scala.io.Source.fromFile(filename, "UTF-8").getLines) {
//     val (id, name, city, country, iata, icao, latitude, longitude, altitude, timezone, dst, tz, ztype, source) = line.split(",").map(_.trim)
//     println(airport)
// }
///////////////////////////////////////////////////////////////

val spark = SparkSession.builder.getOrCreate()
val sqlContext = spark.sqlContext

// airports
val airports = sqlContext.read.format("csv").option("delimiter", ",").option("quote", "").option("header", "true").option("inferSchema", "true").csv("/opt/spark-data/airports-extended.dat")
val routes = sqlContext.read.format("csv").option("delimiter", ",").option("quote", "").option("header", "true").option("inferSchema", "true").csv("/opt/spark-data/routes.dat")
// var airlines = sqlContext.read.format("csv").option("delimiter", ",").option("quote", "").option("header", "true").option("inferSchema", "true").csv("/opt/spark-data/airlines.dat")

val airportsDF = airports.select("*")
// airportsDF.printSchema()

val routesDF = routes.select("*")
// routesDF.printSchema()

// ------------------------ 1. Get max distance of an airport
// val airports_timezone = airportsDF.selectExpr("name as airport_name", "abs(timezone) as abs_timezone").groupBy("airport_name").agg(sum("abs_timezone").as("max_dist")).sort(desc("max_dist"))
// airports_timezone.show()

// --------- 1. using graphx
val justAirports = airportsDF.selectExpr("id as airport_id", "name as airport_name").distinct()

val cities = routesDF.select("src_airport_id").withColumnRenamed("src_airport_id", "airport_id").union(routesDF.select("dst_airport_id").withColumnRenamed("dst_airport_id", "airport_id")).distinct().select(col("airport_id").cast("long").alias("value")).na.drop()
cities.na.drop()

val airportVertices: RDD[(VertexId, String)] = cities.join(justAirports, cities("value") === justAirports("airport_id")).select(col("airport_id").cast("long"), col("airport_name")).na.drop().rdd.map(row => (row.getLong(0), row.getString(1)))
airportVertices.take(10)

val existingRoutesDF = routesDF.select("*").where("src_airport_id != '\\N' and dst_airport_id != '\\N'").na.drop()
val airportEdges:RDD[Edge[Long]] = existingRoutesDF.select(col("src_airport_id").cast("long"), col("dst_airport_id").cast("long")).na.drop().rdd.map(row => Edge(row.getLong(0), row.getLong(1), 1))
airportEdges.take(10)

val defaultStation = ("Missing airport") 
val airportGraph: Graph[String, Long] = Graph(airportVertices, airportEdges, defaultStation)
airportGraph.cache()

println("Total Number of Airports: " + airportGraph.numVertices)
println("Total Number of Routes: " + airportGraph.numEdges)
// sanity check
println("Total Number of Routes in Original Data: " + cities.count)


// ---- query 1
// val ranks = airportGraph.pageRank(0.00001).vertices
// val res = ranks.join(airportVertices)

// with bigest one
// println("The biggest one: ")
// res.sortBy(_._2._1, ascending=false).take(3).foreach(x => println(x._2._2))

// with smallest one
// println("The smallest one: ")
// res.sortBy(_._2._1, ascending=true).take(3).foreach(x => println(x._2._2))

// ---- query 2
val ranks = airportGraph.vertices
println(ranks)


// ---- query 3
// airportGraph.groupEdges((edge1, edge2) => edge1 + edge2).triplets.sortBy(_.attr, ascending=false).map(triplet => "There were " + triplet.attr.toString + " trips from " + triplet.srcAttr + " to " + triplet.dstAttr + ".").take(10).foreach(println)

// for (airport <- airports) {
    // println(airport)
// }
// println(airports)

// val airports = spark.read.format("csv") \
//   .option("sep", ",") \
//   .option("inferSchema", "true") \
//   .option("header", "false") \
//   .load("/opt/spark-data/airports-extended.dat")

// for (airport <- airports) println(airport)