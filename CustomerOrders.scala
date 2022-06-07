import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext

object CustomerOrders extends App{

  Logger.getLogger("org").setLevel(Level.ERROR)

  val sc = new SparkContext("local[*]","CustomerOrders")

  val input = sc.textFile("/Users/swaroopmutalik/Desktop/Technical/BigDataCourse/customerorders.csv")

  val mappedInput = input.map(x=>(x.split(",")(0),x.split(",")(2).toFloat))

  val totalByCustomer = mappedInput.reduceByKey((x,y)=>(x+y))

  val sortedTotal = totalByCustomer.sortBy(x=>x._2,false)

  val result = sortedTotal.collect

  result.foreach(println)
}
