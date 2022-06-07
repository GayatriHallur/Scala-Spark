# Scala-Spark - Understanding Word Count Program Code

This is word count program written in scala using IntelliJ. For this program scala 2.12.15 version is used and spark 3.2.1 version is
installed in the system.

Before starting to understand the code, let us analyse the input file content by taking few input lines into consideration
as shown below..

**best big data hadoop spark training**

**kelly technologies courses**

Data is seperated using space as shown above in the the input file as shown above.

Let us understand word count program written in spark with the help of code.

1. val sc = new SparkContext("local[*]","wordcount")

   Val sc is the entry point to Spark and Pyspark Cluster. Please note that if you are using the spark-shell to write this code, there is 
no need to create sc variable as its already available in spark-shell. 

2. val input =  sc.textFile("/Users/swaroopmutalik/Desktop/Technical/BigDataCourse/search_data.txt") 

   Input content will be loaded to this variable at the time of execution.

3. val flatmap = input.flatMap(x=>x.split(" "))

   flatMap function is used to flatten out the structure of the file. After above operation array of string will be formed for each word as 
shown below:

   **Array(best, big, data, hadoop, spark, training, kelly, technologies, courses, ....)**

4. val words = flatmap.map(x=>x.toLowerCase())

   Using above map operation all the words will be converted to lowercase. This step will help us to avoid duplicates of each word.

   For example, Big and big will be considered as two different words. However, we need to consider those as one.

5. val result = words.map(x =>(x,1))

   This operation will map 1 to each word as shown below:
   
   **Array((best,1), (big,1), (data,1), (hadoop,1), (spark,1), (training,1), (kelly,1), (technologies,1),....)**

6. val finalOutput = result.reduceByKey((x,y)=>(x+y))

   This reduceByKey will help us to aggregate values of each words available on different executor on spark cluster.

7. val sortedResults = finalOutput.sortBy(x=>x._2,false)
   
   This step is used to sort the output in the descending order. This means that the word will maximum occurances will be sorted 
at the top.

8. sortedResults.collect.foreach(println)

   This step helps us to show the result on the console. 

This document will help you to understand the code in better way.

