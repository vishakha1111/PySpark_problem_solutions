from pyspark import SparkContext

sc= SparkContext("local[*]", "movie-ratings")

input = sc.textFile("D:/VISHAKHA/BIG DATA COURSE/Week 9/movie-data.data")

mappedInput = input.map(lambda x : (x.split("\t")[2], 1))

reducedRatings = mappedInput.reduceByKey(lambda x,y : x + y).collect()

for a in reducedRatings:
    print(a)
