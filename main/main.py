import pyspark
import operator
import datetime

sc = pyspark.SparkContext("local", "TaxiNY")
csvFile1 = sc.textFile("../dataset/trip_data_2.csv")

header = csvFile1.first()
print("\n...................................\n")
# print(header)

# Remove the header
csvFile1 = csvFile1.filter(lambda row: row != header)

# print(csvFile1.first())

csvData = csvFile1.map(lambda lines: lines.split(","))
csvData.cache()

dataLength = csvData.count()
print(f"一共有{dataLength}条出行数据\n")

passengerCountRDD = csvData.map(lambda lines: (lines[7], 1))
passengerCount = passengerCountRDD.reduceByKey(operator.add).collect()

# print(sorted(passengerCount))

for i in sorted(passengerCount):
    allPassengerCount = int(i[0]) * int(i[1])

# 乘客人数相关
countString = "人数 次数\n"
with open("./file/passengerCount.txt", "w") as f:
    for i in sorted(passengerCount):
        countString = countString + str(i[0]) + " " + str(i[1]) + "\n"
    countString = countString + "乘客平均数量为：" + str(float(allPassengerCount) / dataLength)
    f.write(countString)

# 一个月中细分的每一天运行次数
dateTimeCountRDD = csvData.map(lambda lines: (datetime.datetime.strptime(lines[5].split(" ")[0], "%Y-%m-%d").date(), 1))
dateTimeCount = dateTimeCountRDD.reduceByKey(operator.add).collect()
countString = "日期 出租车运行次数\n"
with open("./file/dateTimeCount.txt", "w") as f:
    for i in sorted(dateTimeCount):
        countString = countString + str(i[0]) + " " + str(i[1]) + "\n"
    f.write(countString)

# 一天中各个时间段出租车运行的次数
hourTimeCountRDD = csvData.map(lambda lines: (datetime.datetime.strptime(lines[5].split(" ")[1], "%H:%M:%S").hour,1))
hourTimeCount = hourTimeCountRDD.reduceByKey(operator.add).collect()
countString = "时间段 出租车运行次数\n"
with open("./file/hourTimeCount.txt", "w") as f:
    for i in sorted(hourTimeCount):
        countString = countString + str(i[0]) + " " + str(i[1]) + "\n"
    f.write(countString)

# 出租车运行的平均时间
tripTimeTotal= csvData.map(lambda lines: int(lines[8])).sum()
print(f"出租车运行的平均时间为：{float(tripTimeTotal / dataLength)}")