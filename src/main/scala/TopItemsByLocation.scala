import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{SparkSession, Row}
import org.apache.spark.sql.types._
import org.apache.spark.rdd.RDD


object TopItemsByLocation {
  def main(args: Array[String]): Unit = {
    // Initialize SparkSession
    val spark = SparkSession.builder()
      .appName("TopItemsByLocation")
      .getOrCreate()
    
    // Read input paths from arguments
    val inputPathA = args(0) // Path for Parquet File 1
    val inputPathB = args(1) // Path for Parquet File 2
    val outputPath = args(2) // Path for Parquet File 3
    val topX = args(3).toInt // Top X configuration
    
    // Schema for dataset A
    val schemaA = StructType(Seq(
      StructField("geographical_location_oid", LongType, nullable = true),
      StructField("video_camera_oid", LongType, nullable = true),
      StructField("detection_oid", LongType, nullable = true),
      StructField("item_name", StringType, nullable = true),
      StructField("timestamp_detected", LongType, nullable = true)
    ))

    // Read dataset A and B as DataFrame
    val dfA =spark.read.schema(schemaA).parquet(inputPathA)
    val dfB = spark.read.parquet(inputPathB)
    
   // Convert DataFrame A to RDD
    val rddA: RDD[(Long, Long, String)] = dfA.rdd.map(row => (row.getAs[Long]("geographical_location_oid"), 
                                                        row.getAs[Long]("detection_oid"), 
                                                        row.getAs[String]("item_name")))
                                                        
    // map each row to a key-value pair with the "detection_oid" as the key and the row as the value
    // use reduceByKey to remove duplicates based on the key
    // use map to extract the rows from the key-value pairs
    val deduplicatedRDD = rddA.map(row => (row._2, row)).reduceByKey((row1, row2) => row1).map(_._2)
    
    val rddWithItemCount = getItemCount(deduplicatedRDD)
    val topXItemsByLocation = getItemRanks(rddWithItemCount, topX)
    
    // Convert DataFrame B to RDD
    val rddB = dfB.rdd.map(row => (row.getLong(0), row.getString(1)))
    val rankedItemsWithLocation = addLocation(topXItemsByLocation, rddB)
    
    val outputSchema = StructType(Seq(
      StructField("geographical_location", StringType, nullable = true),
      StructField("item_name", StringType, nullable = true),
      StructField("item_rank", StringType, nullable = true)
    ))
    // Convert rankedItemsWithLocation RDD to DataFrame
    val resultWithLocationDF = spark.createDataFrame(rankedItemsWithLocation.map {
      case (geographical_location, item_name, item_rank) => Row(geographical_location, item_rank, item_name )
    }, outputSchema)
    // Write result to output path
    resultWithLocationDF.write.mode("overwrite").parquet(outputPath)
    
    // Stop SparkSession
    spark.stop()
  }
  def getItemCount(deduplicatedRDD: RDD[(Long, Long, String)]): RDD[(Long, Long, String, Int)] = {
     // Map each row in deduplicatedRDD to a tuple of ((geographical_location_oid, item_name), 1)
    val locationItemCounts = deduplicatedRDD.map(row => ((row._1, row._3), 1))

    // Reduce by key to get the count of each item at each location
    val itemCounts = locationItemCounts.reduceByKey(_ + _)

    // Add item_count column to the RDD
    val rddWithItemCount = deduplicatedRDD.map(row => ((row._1, row._3), row)).join(itemCounts)
                                          .map{ case ((location, item), (row, count)) => (location, row._2, item, count) }
    rddWithItemCount
  }

  def getItemRanks(rddWithItemCount: RDD[(Long, Long, String, Int)], X: Int): RDD[(Long, Long, String, Int, String)]={
    // Map each row in rddWithItemCount to ((geographical_location_oid, item_name), (item_count, row))
    val locationItemCounts = rddWithItemCount.map(row => ((row._1, row._3), (row._4, row)))

    // Group by location and sort the items by count in descending order
    val groupedLocations = locationItemCounts.groupByKey().mapValues(_.toList.sortBy(-_._1))

    // Map each group to add rank for each item
    val rankedItems = groupedLocations.flatMapValues { items =>
      items.zipWithIndex.map { case ((count, row), rank) => (count, row, rank + 1) }
    }

    // Filter to keep only top 10 items at each location
    val top10ItemsPerLocation = rankedItems.filter { case (_, (_, _, rank)) => rank <= X }

    // Extract relevant columns and remove temporary keys
    val finalResult = top10ItemsPerLocation.map { case ((location, itemName), (count, row, rank)) => (location, row._2, itemName, count, rank.toString()) }

    finalResult
  }

  def addLocation(rankedItemsRdd:RDD[(Long, Long, String, Int, String)], locationRdd:RDD[(Long, String)]): RDD[(String, String, String)]={
    // locationRDD is from datasetB, where the first element is geographical_location_oid and the second element is location

    // Join ranked RDD with locationRDD
    val resultWithLocation = rankedItemsRdd.map { case (geographical_location_oid, detection_oid, item_name, count, rank) =>
      (geographical_location_oid, ( item_name, rank))
    }.join(locationRdd).map { case (geographical_location_oid,((item_name, rank), location)) =>
      (location, item_name, rank)
    }
    resultWithLocation
  }
}