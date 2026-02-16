from pyspark.sql import SparkSession
from pyspark.sql.functions import transform
"""transform works only on array columns."""

spark = SparkSession.builder.getOrCreate()
data = [
    (1, [1, 2, 3]),
    (2, [4, 5, 6]),
    (3, [7, 8, 9]),
]

df = spark.createDataFrame(data, ["id", "numbers"])
df=df.withColumn("numbers_multiply", transform("numbers", lambda x:x*5))
df.show()
spark.stop()