
package rdd.count

import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.{SparkConf, SparkContext}

object CountExample_new {

  def main(args: Array[String]) {
    Logger.getLogger("org").setLevel(Level.ERROR)
    val conf = new SparkConf().setAppName("count")
    val sc = new SparkContext(conf)

    println("Hello world New")
    val inputWords = List("spark", "hadoop", "spark", "hive", "pig", "cassandra", "hadoop")
    val inputNumbers = List(11,22,33,44,55,66,77,88,99,88,77,66,55)

    val wordRdd = sc.parallelize(inputWords)
    val numRdd = sc.parallelize(inputNumbers)

    println("word Array Count: " + wordRdd.count() + "\n number Array count " + numRdd.count() )

    //val counts = new collection.mutable.HashMap[String,Int].withDefaultValue(0)
    val wordCountByValue = wordRdd.countByValue()
    val numCountByValue = numRdd.countByValue()

    println( "scala regular map output of wordRDD.countByValue (its an action): \n" + wordCountByValue)
    println( "scala regular map output of numRDD.countByValue  (its an action): \n" + numCountByValue)
    
    println("wordRDD.countByValue (println):  ") 
    for ((word, count) <- wordCountByValue) println( word + " : " + count)


    println("numRDD.countByValue (println):  ") 
    for ((word, count) <- numCountByValue) println( word + " : " + count)

    println("actioned data conversion back to RDD ")
    
    var actioned_toWordRDD = sc.parallelize(wordCountByValue.toSeq.map(x=>(x._1,x._2)))
    var actioned_toNumRDD = sc.parallelize(numCountByValue.toSeq.map(x=>(x._1,x._2)))

    actioned_toWordRDD.saveAsTextFile("wordRdd_test.txt")
    actioned_toWordRDD.saveAsTextFile("numRdd_test.txt")

  }

}

