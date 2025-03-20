from pyspark.sql import SparkSession

# create a SparkSession
spark = SparkSession.builder.appName("My First Spark Job").getOrCreate()

# create a sample DataFrame
data = spark.createDataFrame([
    ("John", 25),
    ("Mary", 31),
    ("David", 42)
], ["Name", "Age"])

# print the DataFrame
data.show()

# stop the SparkSession
spark.stop()