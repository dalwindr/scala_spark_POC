import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD
import org.apache.spark.rdd.RDD._
import org.apache.spark.rdd.PairRDDFunctions
import scala.collection.Map
import org.apache.spark.SparkContext.rddToPairRDDFunctions

class newExample(sc: SparkContext) {
	def run(t: String, u: String): RDD[(String,String)] = {
		var transdata = sc.textFile(t)
		var readTrans = transdata.map{ line => val p = line.split("\t") ;( p(2).toInt, p(1).toInt)}
		
		var userdata = sc.textFile(u)
		var readUsers = userdata.map{ line => val p = line.split("\t") ;( p(0).toInt, p(3).toString)}
	
		val result = processData(readTrans,readUsers)
		//val result1 = readTrans.leftOuterJoin(readUsers).values.distinct
		//val result = result1.countByKey
		sc.parallelize (result.toSeq).map(x => (x._1.toString,x._1.toString))
	}

   def processData(t: RDD[(Int,Int)], u: RDD[(Int, String)]): Map[ Int , Long] = {
  		var joinout = t.leftOuterJoin(u).values.distinct
  		joinout.countByKey 
    }
	
}

object newExample{

  def main(args:Array[String]) {
  	val conf = new SparkConf().setAppName("wordCount") 
	
	val sc = new SparkContext(conf)


	// Load our input data.
	val input = sc.textFile(args(0))

	// Split it up into words.
	val words = input.flatMap(line => line.split(","))

	println("Total Number of lines is :", words.count())
	

	val _wordcounts = words.map(w => (w, 1)).reduceByKey{case(x, y) => x + y}.count() // Save the word count back out to a text file, causing evaluation. counts.saveAsTextFile(outputFile)


	println("Total Number of words is :", _wordcounts)

	
	//val transfiledata = sc.textFile("hdfs://ip-172-31-35-141.ec2.internal:8020/apps/hive/warehouse/dalwin.db/input_dir/transactions.txt")
	//val userfiledata = sc.textFile("hdfs://ip-172-31-35-141.ec2.internal:8020/apps/hive/warehouse/dalwin.db/input_dir/users.txt")


	val transfiledata = "/apps/hive/warehouse/dalwin.db/input_dir/transactions.txt"
	val userfiledata = "/apps/hive/warehouse/dalwin.db/input_dir/users.txt"
	
	
	var obj_example = new newExample(sc)
	var result = obj_example.run(transfiledata, userfiledata)
	result.saveAsTextFile("TransUserJoin.txt")
	//context.stop()
}
}





