from pyspark.sql.functions import map_from_entries, col
from pyspark.sql import SparkSession

"""
note:
map_from_entries requeries
An array
Each element must be a struct(key, value)
"""

spark = SparkSession.builder.getOrCreate()
data = [
    (1, [("math", 90), ("english", 85)]),
    (2, [("math", 75), ("english", 95)])
]

df = spark.createDataFrame(data, ["id", "subjects"])
df_map = df.withColumn(
    "subjects_map",
    map_from_entries("subjects")
)

df_map.show(truncate=False)
#df.show(truncate=False)

