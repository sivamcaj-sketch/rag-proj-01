from pyspark.sql import SparkSession
from pyspark.sql.functions import struct, transform, map_from_entries, col

spark = SparkSession.builder.getOrCreate()

data = [
    (1, ["math", "english"], [90, 85]),
    (2, ["telugu", "science"], [90, 85])
]

df = spark.createDataFrame(data, ["id", "subjects", "marks"])
df2 = df.withColumn(
    "entries",
    transform(
        "subjects",
        lambda x, i: struct(x.alias("key"), col("marks")[i].alias("value"))
    )
)
#df2.show(truncate=False)
# Convert to map
df3 = df2.withColumn("subject_map", map_from_entries("entries"))
df3.show(truncate=False)