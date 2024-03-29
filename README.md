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