import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext

object WordCount  extends App{

  Logger.getLogger("org").setLevel(Level.ERROR)

  val sc = new SparkContext("local[*]","wordcount")

  val input =  sc.textFile("/Users/swaroopmutalik/Desktop/Technical/BigDataCourse/search_data.txt")

  val flatmap = input.flatMap(x=>x.split(" "))

  val words = flatmap.map(x=>x.toLowerCase())

  val result = words.map(x =>(x,1))

  val finalOutput = result.reduceByKey((x,y)=>(x+y))

  val sortedResults = finalOutput.sortBy(x=>x._2,false)

  sortedResults.collect.foreach(println)
}
