# ScalaProject

A spark-based transformation program (using only RDD) on Scala.  
Task: To rank the top few items found at each location. 

## Local package versions:
Java: jdk22  
sbt: 1.9.8  
Hadoop: 3.3.0  
Spark: 3.5.1  
Scala: 2.13.12  

## Distributed Computing Tasks I
### a. Style Checking 
Checked using 'scalastyle_2.13-1.5.1-assembly.jar'. Config file can be found in default_config.xml  
(Consists of all default rules provided on scalaStyle, minus header rule including license info)
``` cmd
java -jar scalastyle_2.13-1.5.1-assembly.jar --config default_config.xml src/main/scala
```
### b. Integration and Unit testing
#### Unit Tests
Tests can be found under 'src/test/scala' and tests the following functions:
1. "getItemCount": calculates the count of items for each geographical location
2. "getItemRanks": ranks items for each geographical location
3. "addLocation": combines location name to ranked items based on geographic_id

#### Integration Test
Sample parquet files have been provided in /datasets folder  
Dataset A: file with 5000 rows, 10 different item_name and 100 geographical_location_oid (1-100)  
Dataset B: file with 100 rows, with geographical_location_oid (1-100) and different geographical_location (string with 20 chars)  
Dataset C: file with 4 rows, to test out same detection_oid  

``` 
dataset_c = {
    'geographical_location_oid': [1, 2, 1, 1],
    'video_camera_oid': [101, 102, 101, 101],
    'detection_oid': [1001, 1002, 1001, 1003],  # Duplicate detection_oid
    'item_name': ['Item 1', 'Item 2', 'Item1', 'Item 1'],
    'timestamp_detected': [1633438400, 1633442000, 1633438400, 1633438400]
}
```  

Commands to compile and run:

```
sbt package
```
Resulting jar file will be found at 'target/scala-2.13/topitemsbylocation_2.13-0.1.0-SNAPSHOT.jar'  
Running with Dataset A and B with topX = 10, output saved in /output folder
```
C:\Spark\bin\spark-submit --class TopItemsByLocation --master local[*] target/scala-2.13/topitemsbylocation_2.13-0.1.0-SNAPSHOT.jar datasets/sample_dataset_a.parquet datasets/sample_dataset_b.parquet output/ 10
```

### g. Design Considerations

Design considerations can be found in 'Part_1_design_considerations.txt'

## Distributed Computing Task II 

Below shows the code snippets changes if there was a data skew as required in Part 1.  
Answers to Part 2 can be found in sorting.pdf
### Method 1: Salting
To address data skew in one of the geographical locations in Dataset A, salting can be used by adding a random prefix to the geographical_location_oid key to distribute the data evenly across partitions and reduce skew. The partitionRDDByLocation method can be changed to partitionRDDByLocationWithSalting as shown in the code snippet below. The original partitionRDDByLocation method can be replaced with this new implementation to handle data skew more effectively. The number of partitions (numPartitions) should be changed based on the severity of the data skew (I have only doubled it).

``` scala
import scala.util.Random

def partitionRDDByLocationWithSalting(rddA:RDD[(Long, Long, String)], spark: SparkSession): RDD[(Long, Long, String)] = {
    // Convert RDD to PairRDD with geographical_location_oid as key
    val pairRDDA: RDD[(Long, (Long, String))] = rddA.map { case (geographical_location_oid, detection_oid, item_name) =>
      (geographical_location_oid, (detection_oid, item_name))
    }

    // Increase number of partitions to use to mitigate skew
    val numPartitions = spark.sparkContext.defaultParallelism * 2

    // Partition PairRDD A by geographical_location_oid with salting
    val partitionedPairRDDA = pairRDDA.partitionBy(new Partitioner {
      override def numPartitions: Int = numPartitions

      override def getPartition(key: Any): Int = {
        val hashPartitioner = new HashPartitioner(numPartitions)
        val saltedKey = s"${Random.nextInt(numPartitions)}_$key"
        hashPartitioner.getPartition(saltedKey)
      }
    })

    // Convert PairRDD back to original structure
    val partitionedRDDA: RDD[(Long, Long, String)] = partitionedPairRDDA.map { case (geographical_location_oid, (detection_oid, item_name)) =>
      (geographical_location_oid, detection_oid, item_name)
    }

    partitionedRDDA
  }
```

### Method 2: Filtering and pre-aggregating
A filter can be applied to process only the relevant data for the location with data skew separately.
The geographical location with data skew is identified by analyzing the distribution of data across locations in Dataset A. Then, Dataset A is filtered for relevant data from the skewed location, and processed separately to compute the top X items for the skewed location, and similarly process the non-skewed locations. The results from processing the skewed location and non-skewed locations is then merged to obtain the same final result.

``` scala

    def identifySkewedLocation(rddA: RDD[(Long, Long, String)]): Long = {
        //  identifies the location with the highest count (assume there is only one)
        val skewedLocation = rddA.map(row => (row._1, 1))
            .reduceByKey(_ + _)
            .map(_.swap)
            .max()._2
        skewedLocation
        }
    
    def processDatasetA(rddA: RDD[(Long, Long, String)], topX: Int): RDD[(Long, String, Int)] = {
    // Process Dataset A to compute top X items for the given geographical location
        val deduplicatedRDD = rddA.map(row => (row._2, row)).reduceByKey((row1, row2) => row1).map(_._2)
        val rddWithItemCount = getItemCount(deduplicatedRDD)
        val topXItemsByLocation = getItemRanks(rddWithItemCount, topX)
        topXItemsByLocation
      }

    // After reading both datasets
    // Identify the geographical location with data skew
    val skewedLocation = identifySkewedLocation(rddA)

    // Filter Dataset A to process only the relevant data for the skewed location
    val filteredRddA = rddA.filter { case (geographical_location_oid, _, _) =>
      geographical_location_oid == skewedLocation
    }

    // Process the filtered Dataset A separately
    val resultForSkewedLocation = processDatasetA(filteredRddA, topX)

    // Process the rest of Dataset A
    val resultForNonSkewedLocations = processDatasetA(rddA.filter { case (geographical_location_oid _, _) =>
      geographical_location_oid != skewedLocation
    }, topX)

    // Merge the results from both computations
    val finalResult = resultForSkewedLocation.union(resultForNonSkewedLocations)

    // continue with writing output

```


## Data Architecture Design

Details can be found in design.pdf

