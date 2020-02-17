from pyspark import SparkContext

sc = SparkContext.getOrCreate()

# x = sc.textFile("/home/ansh/PycharmProjects/ICP4/data.csv") \
#     .map(lambda line: line.split(",")) \
#     .map(lambda line: (line[3], 1)) \
#     .reduceByKey(lambda a, b: a + b)
# print(x.collect())

# x = sc.textFile("/home/ansh/PycharmProjects/ICP4/data.csv") \
#      .map(lambda line: line.split(",")) \
#      .map(lambda line: (line[7])) \
#      .distinct()
# print(x.collect())
# print(x.count())
# print(x.take(2))

# x = sc.textFile("/home/ansh/PycharmProjects/ICP4/data.csv") \
#     .map(lambda line: line.split(",")) \
#     .map(lambda line: (line[3]))
# print(x.collect())
x = sc.textFile("/home/ansh/PycharmProjects/ICP4/data.csv") \
    .map(lambda line: line.split(","))
df4 = x.filter(lambda line: "105.5" in line).collect()
print(df4)
