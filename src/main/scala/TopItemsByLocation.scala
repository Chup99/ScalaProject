// Main object class to get rankings of items detected at each location

import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.{SparkSession, Row}
import org.apache.spark.sql.types.{StructType, StructField, LongType, StringType}
import org.apache.spark.rdd.RDD
import org.apache.spark.HashPartitioner
import org.apache.spark.Partitioner

object Constants {
  // Schema for dataset A
  val itemNameString = "item_name"
  val SchemaA = StructType(Seq(
      StructField("geographical_location_oid", LongType, nullable = true),
      StructField("video_camera_oid", LongType, nullable = true),
      StructField("detection_oid", LongType, nullable = true),
      StructField(itemNameString, StringType, nullable = true),
      StructField("timestamp_detected", LongType, nullable = true)
    ))

  val OutputSchema = StructType(Seq(
      StructField("geographical_location", StringType, nullable = true),
      StructField("item_rank", StringType, nullable = true),
      StructField(itemNameString, StringType, nullable = true)
  ))
}

object TopItemsByLocation {
  def main(args: Array[String]): Unit = {
    // Initialize SparkSession
    val spark = SparkSession.builder()
      .appName("TopItemsByLocation")
      .getOrCreate()

    // Read input paths from arguments
    val inputPathA = args(i=0) // Path for Parquet File 1
    val inputPathB = args(i=1) // Path for Parquet File 2
    val outputPath = args(i=2) // Path for Parquet File 3
    val topX = args(i=3).toInt // Top X configuration

    // Read dataset A and B as DataFrame
    val dfA =spark.read.schema(Constants.SchemaA).parquet(inputPathA)
    val dfB = spark.read.parquet(inputPathB)

   // Convert DataFrame A to RDD
    val rddA: RDD[(Long, Long, String)] = dfA.rdd.map(row => (row.getAs[Long]("geographical_location_oid"),
                                                        row.getAs[Long]("detection_oid"),
                                                        row.getAs[String](Constants.itemNameString)))
    val partitionedRDDA = partionRDDByLocation(rddA, spark)
    // map each row to a key-value pair with the "detection_oid" as the key and the row as the value
    // use reduceByKey to remove duplicates based on the key
    // use map to extract the rows from the key-value pairs
    val deduplicatedRDD = partitionedRDDA.map(row => (row._2, row)).reduceByKey((row1, row2) => row1).map(_._2)

    val rddWithItemCount = getItemCount(deduplicatedRDD)
    val topXItemsByLocation = getItemRanks(rddWithItemCount, topX)

    // Convert DataFrame B to RDD
    val rddB = dfB.rdd.map(row => (row.getLong(i=0), row.getString(i=1)))
    val rankedItemsWithLocation = addLocation(topXItemsByLocation, rddB)

    // Convert rankedItemsWithLocation RDD to DataFrame
    val resultWithLocationDF = spark.createDataFrame(rankedItemsWithLocation.map {
      case (geographical_location, item_name, item_rank) => Row(geographical_location, item_rank, item_name)
    }, Constants.OutputSchema)
    // Write result to output path
    resultWithLocationDF.write.mode("overwrite").parquet(outputPath)

    // Stop SparkSession
    spark.stop()
  }

  def partionRDDByLocation(rddA:RDD[(Long, Long, String)], spark: SparkSession): RDD[(Long, Long, String)]={
     // Convert RDD to PairRDD with geographical_location_oid as key
    val pairRDDA: RDD[(Long, (Long, String))] = rddA.map { case (geographical_location_oid, detection_oid, item_name) =>
      (geographical_location_oid, (detection_oid, item_name))
    }

    // Partition PairRDD A by geographical_location_oid
    val partitionedPairRDDA = pairRDDA.partitionBy(new HashPartitioner(spark.sparkContext.defaultParallelism))
    // Convert PairRDD back to original structure
    val partitionedRDDA: RDD[(Long, Long, String)] = partitionedPairRDDA.map { case (geographical_location_oid, (detection_oid, item_name)) =>
      (geographical_location_oid, detection_oid, item_name)
    }
    partitionedRDDA
  }
  def getItemCount(deduplicatedRDD: RDD[(Long, Long, String)]): RDD[(Long, String, Int)] = {
     // Map each row in deduplicatedRDD to a tuple of ((geographical_location_oid, item_name), 1)
    val locationItemCounts = deduplicatedRDD.map { case (geographic_id, _, item_name) => ((geographic_id, item_name), 1) }

    // Reduce by key to get the count of each item at each location
    val itemCounts = locationItemCounts.reduceByKey(_ + _)

    // Rearrange the data to have geographic_id as the key
    val rddWithItemCount = itemCounts.map { case ((geographic_id, item_name), count) => (geographic_id, item_name, count) }
    rddWithItemCount
  }

  def getItemRanks(rddWithItemCount: RDD[(Long, String, Int)], topX: Int): RDD[(Long, String, String)]={
    // Map each row in rddWithItemCount to ((geographical_location_oid), (item_name, item_count))
    val locationItemCounts = rddWithItemCount.map(row => ((row._1), (row._2, row._3)))

    // Group by location and sort the items by count in descending order
    val groupedLocations = locationItemCounts.groupByKey().flatMapValues(_.toList.sortBy(-_._2))

    // Get top X items per location
    val rankedItems = groupedLocations.groupByKey().flatMapValues(_.zipWithIndex.filter(_._2 < topX)
                      .map { case ((itemName, itemCount), rank) => (itemName, rank + 1) })

    // Extract relevant columns and remove temporary keys
    val finalResult = rankedItems.map { case (geographicalId, (itemName, rank)) => (geographicalId, itemName, rank.toString()) }

    finalResult
  }

  def addLocation(rankedItemsRdd:RDD[(Long, String, String)], locationRdd:RDD[(Long, String)]): RDD[(String, String, String)]={
    // locationRDD is from datasetB, where the first element is geographical_location_oid and the second element is location

    // Join ranked RDD with locationRDD
    val resultWithLocation = rankedItemsRdd.map { case (geographical_location_oid, item_name, rank) =>
      (geographical_location_oid, ( item_name, rank))
    }.join(locationRdd).map { case (geographical_location_oid,((item_name, rank), location)) =>
      (location, item_name, rank)
    }
    resultWithLocation
  }
}
