from pyspark import SparkContext

sc= SparkContext("local[*]", "customer-orders")

input = sc.textFile("D:/VISHAKHA/BIG DATA COURSE/Week 9/customer-orders.csv")

mappedInput = input.map(lambda x : (x.split(",")[0], float(x.split(",")[2])))

totalByCustomer = mappedInput.reduceByKey(lambda x, y : x+y)

result = totalByCustomer.sortBy(lambda x : x[1], False)

final = result.collect()

for a in final:
    print(a)

