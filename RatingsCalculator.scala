import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext

object RatingsCalculator extends App{

  Logger.getLogger("org").setLevel(Level.ERROR)

  val sc = new SparkContext("local[*]","ratingscal")

  val input = sc.textFile("/Users/swaroopmutalik/Desktop/Technical/BigDataCourse/moviedata.data")

  val mappedInput = input.map(x=>x.split("\t")(2))

  val countBy = mappedInput.countByValue()

  countBy.foreach(println)

 // val mapped = mappedInput.map(x=>(x,1))

 // val results = mapped.reduceByKey((x,y)=>(x+y))

  //val sortedresults = results.sortBy(x=>x._1,false)

  //sortedresults.collect.foreach(println)

}
