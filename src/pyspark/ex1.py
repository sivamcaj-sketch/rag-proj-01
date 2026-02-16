from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = SparkSession.builder.getOrCreate()
data = [
    (1, "Alice", 25),
    (2, "Bob", 30)
]

df = spark.createDataFrame(data, ["id", "name", "age"])
#df.show()

df_struct = df.withColumn("person_info", struct("name", "age"))
df_struct.show(truncate=False)

