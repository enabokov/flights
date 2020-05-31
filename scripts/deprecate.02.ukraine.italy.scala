import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SparkSession

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
// val justAirports = airportsDF.selectExpr("id as airport_id", "name as airport_name").distinct()
val justCountries = airportsDF.selectExpr("id as airport_id", "country").distinct()

val cities = routesDF.select("src_airport_id").withColumnRenamed("src_airport_id", "airport_id").union(routesDF.select("dst_airport_id").withColumnRenamed("dst_airport_id", "airport_id")).distinct().select(col("airport_id").cast("long").alias("value")).na.drop()
cities.na.drop()

// val airportVertices: RDD[(VertexId, String)] = cities.join(justAirports, cities("value") === justAirports("airport_id")).select(col("airport_id").cast("long"), col("airport_name")).na.drop().rdd.map(row => (row.getLong(0), row.getString(1)))
// airportVertices.take(10)

val countryVertices: RDD[(VertexId, String)] = cities.join(justCountries, cities("value") === justCountries("airport_id")).select(col("airport_id").cast("long"), col("country")).na.drop().rdd.map(row => (row.getLong(0), row.getString(1)))
countryVertices.take(10)

val existingRoutesDF = routesDF.select("*").where("src_airport_id != '\\N' and dst_airport_id != '\\N'").na.drop()
val airportEdges:RDD[Edge[Long]] = existingRoutesDF.select(col("src_airport_id").cast("long"), col("dst_airport_id").cast("long")).na.drop().rdd.map(row => Edge(row.getLong(0), row.getLong(1), 1))
airportEdges.take(10)

val defaultStation = ("Missing airport") 
val airportGraph: Graph[String, Long] = Graph(countryVertices, airportEdges, defaultStation)
airportGraph.cache()

// println("Total Number of Airports: " + airportGraph.numVertices)
// println("Total Number of Routes: " + airportGraph.numEdges)


// ---- query 2
println()
// airportGraph.triplets.filter(triplet => triplet.srcAttr.toString == "Canada" ).map(triplet => "There were " + triplet.attr.toString + " trips from " + triplet.srcAttr + " to " + triplet.dstAttr + ".").collect().take(10).foreach(println)
// airportGraph.vertices.filter(route => route._1 == 691).take(10).foreach(println)

val ukraine_id = 2939
val italyId = countryVertices.filter(route => route._2 == "Italy").map(row => row.getLong(0))
// airportGraph.vertices.filter(route => route._1 == ukraineId) || route._2.toString.equalsIgnoreCase("Italy"))
// airportGraph.edges.filter(route => route.srcId == ukraineId && route.dstId == italyId).take(10).foreach(println)
// airportGraph.edges.collect.foreach(println)

// val airportGraphExt = airportGraph.joinVertices(countryVertices)



// ---- query 3
// airportGraph.groupEdges((edge1, edge2) => edge1 + edge2).triplets.sortBy(_.attr, ascending=false).map(triplet => "There were " + triplet.attr.toString + " trips from " + triplet.srcAttr + " to " + triplet.dstAttr + ".").take(10).foreach(println)
