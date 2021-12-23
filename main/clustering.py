from pyspark.mllib.linalg import Vectors
from pyspark.mllib.clustering import KMeans
import pyspark
import operator
import datetime

sc = pyspark.SparkContext("local", "TaxiNY")
# sc = pyspark.SparkContext.getOrCreate("local", "TaxiNY")

csvFile1 = sc.textFile("../dataset/trip_data_1.csv")

header = csvFile1.first()
print("\n...................................\n")
# print(header)

# Remove the header
csvFile1 = csvFile1.filter(lambda row: row != header)

# print(csvFile1.first())

csvData = csvFile1.map(lambda lines: lines.split(","))
csvData.cache()


correctLocationData = csvData.filter(lambda line: -75 < float(line[10]) < -70 and 36 < float(line[11]) < 45)
pickUpLocation = correctLocationData.map(lambda lines: Vectors.dense([float(lines[10]), float(lines[11])]))
# print(pickUpLocation.collect())

KmeansCenter = KMeans.train(pickUpLocation, k=5, maxIterations=10)
countString = "经度 纬度 \n"
with open("file/pickUpLocation.txt", "w") as f:
    for i in KmeansCenter.clusterCenters:
        countString = countString +str(i[0]) + " " + str(i[1]) + "\n"
        print(countString)
    f.write(countString)


print("kan sei")
