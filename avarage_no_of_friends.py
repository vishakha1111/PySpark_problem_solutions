from pyspark import SparkContext

def parse_line(line):
    fields = line.split(",")
    age = int(fields[2])                    #3rd column
    numFriends = int(fields[3])
    return (age, numFriends)


sc= SparkContext("local[*]", "avaerage no. of friends by age")

input = sc.textFile("D:/VISHAKHA/BIG DATA COURSE/Week 9/friends-data.csv")

rdd1 = input.map(parse_line)
#(33, 385) input
#(33, (385,1))  output

#In scala, we used to access the elements of tuple using x._1, x._2
#In python, we acces the elements of tuple using x[0], x[1]
rdd2 = rdd1.mapValues(lambda x:(x, 1)).reduceByKey(lambda x, y: (x[0] + y[0], x[1] + y[1]))

#(33, (3000, 5) Input
#(33, (3000/5) Output
rdd3 = rdd2.mapValues(lambda x: x[0]/ x[1])

result = rdd3.collect()

for a in result:
    print(a)

