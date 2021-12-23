import pyspark


sc = pyspark.SparkContext("local", "123")

textFile = sc.textFile(r"README.md")
print(textFile.count())