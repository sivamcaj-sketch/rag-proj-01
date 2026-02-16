from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = SparkSession.builder.getOrCreate()
data = [
    (1, "Alice", 25),
    (2, "Bob", 30),
    (3, "Charlie", 40),
    (4, "David", 50),
    (5, "Emma", 60),
    (6, "Frank", 70),
    (7, "George", 80),
    (8, "Harriet", 90)
]

df = spark.createDataFrame(data, ["id", "name", "age"])
#df.show()

df_struct = df.withColumn("person_info", struct("name", "age"))
df_struct.show(truncate=False)

