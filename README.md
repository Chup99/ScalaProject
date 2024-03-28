# ScalaProject

Local package versions:
Java: jdk22
sbt: 1.9.8
Hadoop: 3.3.0
Spark: 3.5.1
Scala: 2.13.12

commands to run:

sbt package

C:\Spark\bin\spark-submit --class TopItemsByLocation --master local[*] target/scala-2.13/topitemsbylocation_2.13-0.1.0-SNAPSHOT.jar datasets/sample_dataset_a.parquet datasets/sample_dataset_b.parquet output/ 10