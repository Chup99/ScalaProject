import org.scalatest._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class TopItemsByLocationSpec extends AnyFlatSpec with Matchers with BeforeAndAfterAll {
  
  var spark: SparkSession = _

  override def beforeAll(): Unit = {
    // Initialize SparkSession for testing
    spark = SparkSession.builder()
      .appName("TopItemsByLocationTest")
      .master("local[2]") // Set master to local with 2 cores for testing
      .getOrCreate()
  }

  override def afterAll(): Unit = {
    // Stop SparkSession after testing
    spark.stop()
  }

  "getItemCount" should "correctly calculate the count of items for each geographical location" in {
   val inputRDD: RDD[(Long, Long, String)] = spark.sparkContext.parallelize(Seq(
      (1L, 1L, "Item1"),
      (1L, 2L, "Item1"),
      (1L, 3L, "Item2"),
      (2L, 4L, "Item1"),
      (2L, 5L, "Item2"),
      (2L, 6L, "Item2"),
      (2L, 7L, "Item3")
    ))
    val outputRDD = TopItemsByLocation.getItemCount(inputRDD)

    // Write assertions to check if counts are as expected
    // Example assertions:
    outputRDD.collect() should contain theSameElementsAs Seq(
      (1L, "Item1", 5),
      (2L, "Item2", 3),
      // Add more expected results as needed
    )
  }

  "getItemRanks" should "correctly rank items for each geographical location" in {
   val inputRDD: RDD[(Long, String, Int)] = spark.sparkContext.parallelize(Seq(
      (1L, "Item1", 5),
      (1L, "Item2", 3),
      (2L, "Item1", 4),
      (2L, "Item2", 6),
      (2L, "Item3", 2)
    ))

    val outputRDD = TopItemsByLocation.getItemRanks(inputRDD, 3) // Assuming top 3 items

    // Write assertions to check if ranks are as expected
    // Example assertions:
    outputRDD.collect() should contain theSameElementsAs Seq(
      (1L, "Item1", "1"),
      (1L, "Item2", "2"),
      // Add more expected results as needed
    )
  }

  "addLocation" should "correctly add location information to ranked items" in {
    val rankedItemsRDD: RDD[(Long, String, String)] = spark.sparkContext.parallelize(Seq(
      (1L, "Item1", "1"),
      (1L, "Item2", "2"),
      (2L, "Item1", "1"),
      (2L, "Item2", "2"),
      (2L, "Item3", "3")
    ))

    val locationRDD: RDD[(Long, String)] = spark.sparkContext.parallelize(Seq(
      (1L, "Location1"),
      (2L, "Location2")
    ))

    val outputRDD = TopItemsByLocation.addLocation(rankedItemsRDD, locationRDD)

    // Write assertions to check if locations are correctly added
    // Example assertions:
    outputRDD.collect() should contain theSameElementsAs Seq(
      ("Location1", "Item1", "1"),
      ("Location2", "Item2", "2"),
      // Add more expected results as needed
    )
  }
}