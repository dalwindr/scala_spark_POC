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

//PartitionerAwareUnionRDD
//getPreferredLocations

//sc.defaultParallelism
//hpRDD.mapPartitionsWithIndex{case (k,rows) => Iterator((k,rows.size))}.collect()
//mapPartitions
//mapPartitionsWithIndex
//foreachPartition
//skewed_large_rdd1.foreachPartition{records => records.foreach(record => println(record))}

//mytestdf.rdd.sample(true,.5).mapPartitions{x=> Iterator(x.size)}.collect  

// skewed_large_rdd1.mapPartitionsWithIndex{ (index: Int, it: Iterator[Int]) => Iterator((index,it.size))}.collect()
// var newrdd= skewed_large_rdd1.mapPartitionsWithIndex{ (index: Int, it: Iterator[Int]) => it.toList.map(x=>{println(index,x)}).iterator }.collect()

//skewed_large_rdd1.glom()

//skewed_large_rdd1.mapPartitionsWithIndex{ (index: Int, it: Iterator[Int]) => Iterator((index,it.toList))}.collect()

//for (x <- (0 to 1000).iterator) { if(x==1000)println(x)}


//skewed_large_rdd1.mapPartitionsWithIndex{ (index: Int, it: Iterator[Int] ) => if (index==1) it.toList.iterator else { println(it.toList.iterator);it.toList.iterator} }.collect()


//val rdd = sc.parallelize(0 to 10, 8)

//rdd.coalesce(numPartitions=8, shuffle=false) 
//res1.toDebugString

//rdd.coalesce(numPartitions=8, shuffle=true)

//skewed_large_rdd1.mapPartitions(itr => Iterator(itr.toList)).collect()

println("Question:1 , what is the difference the coalease and repartition? ")
//  1) coalesce is use to reduce the number of partition
//  repartition is used to increase/decrease the number of partition
//  2) coalesce tries to reduce the network traffice but repartition generate result in full shuffle and generate network traffice
//  3) coalesce allow to create unequal number of partition , repartition preservesPartitioning
//  4) Coalesce can create uneven partitions, but repartition creates equal size partitions
// 5) coalease some time create single file and repartition create multiple partitions

// var mytestdf = (1 to 100).zip(100 to 200).toDF("mytest")
//  mytestdf.rdd.partitions.size  // 4

//Each partition is a separate  CSV (i.e 4) file when you write a DataFrame to disc.

//mytestdf.write.csv(“/Users/powers/Desktop/spark_output/numbers”)

//  var mytestdf8 = mytestdf.repartition(8)
//  var mytestdf12 = mytestdf.repartition(12)
//  mytestdf8.partitions.size    // 4
//  mytestdf12.partitions.size  //12
/*  var mytestdf12coaleasce = mytestdf.coalesce(12)
3327  var mytestdf2coaleasce = mytestdf.coalesce(2)

scala> mytestdf12coaleasce.rdd.partitions.size
res553: Int = 4

scala> mytestdf2coaleasce.rdd.partitions.size
res554: Int = 2
*/

"""Please note that Spark disables splitting for compressed files and creates RDDs with only 1 partition. In such cases, it’s helpful to use sc.textFile('demo.gz') and do repartitioning using rdd.repartition(100) as follows:
rdd = sc.textFile('demo.gz')
rdd = rdd.repartition(100)"""

//lines.repartition(5).count
//Note: here
  //1. map instantiates a MapPartitionsRDD with default partition persistence false:
  //2. mapValues instantiates a MapPartitionsRDD with partition persistence set to true:
  //3. filter instantiates a MapPartitionsRDD with partition persistence set to true:


println("Question 1) there are 1000 small files in directory how do you load in the hadoop? what is the use of mapPartitions")
//  Ans:  stichedlot = sc.wholeFileText("directory/path",4)
    // a) var whole_textf1= sc.wholeTextFiles("/Users/keeratjohar2305/Desktop/practice/Loan_prediction/splits/*")

   // parallell extraction of data  from paritioned RDD to CSV  --mapPartitions
    // b) var outRDD= whole_textf1.mapPartitions(itr=> Iterator( itr.toList.map(meta=> meta._2).mkString )).saveAsTextFile("TEST1.file1")

   // parallell extraction of selected columns from paritioned RDD to CSV  --mapPartitions
    // c)  whole_textf1.mapPartitions(itr=> Iterator( itr.toList.map(_._2).mkString.split("\\r?\\n").map(x=>List( x.split(",")(0) , x.split(",")(1)).mkString(",")).mkString("\n"))).saveAsTextFile("TEST1.file2")

   // extraction the selected partition data from partitioned RDD
   //whole_textf1.mapPartitionsWithIndex((idx,itr)=>  if (idx==0) itr.toList.iterator else {println("no data");List().iterator}).saveAsTextFile("TEST1.file5")
   // OR
   // whole_textf1.mapPartitionsWithIndex((idx,itr)=>  if (idx==0) itr.toList.iterator else Iterator(),true).saveAsTextFile("TEST1.file15")
   // var testrdd= whole_textf1.mapPartitionsWithIndex((idx,itr)=>  if (idx==0) itr.toList.iterator else Iterator(),true)
   	//testrdd.mapPartitionsWithIndex((idx,itr)=>  if (idx==1) itr.toList.iterator else Iterator(),true).foreach(println) // return zero rcords for first partition

/*
	 val input = sc.wholeTextFiles(inputFile)
      val result = input.mapValues{y =>
        val nums = y.split(" ").map(_.toDouble)
        nums.sum / nums.size.toDouble
      }

 */

println("fetch the few percent records fo total records in RDD of one partitions")
   // var percentTofetch=(testrdd.count()*0.10).toInt
   // testrdd.takeSample(false, percentTofetch).foreach(println) 

 
println("what is the difference between map and flatMap")
// scala --
 //  var x= List("This is dalwinder singh","I am living in bangalore")
// x.flatMap(x=> x.split(" ").toList.map(w=>w.toUpperCase()))
// x.map(x=> x.toUpperCase().split(" ").mkString(","))   
// x.map(x=> x.toUpperCase().split(" ")).flatten

// RDD --
 //  var x= sc.parallelize(List("This is dalwinder singh","I am living in bangalore"),2)   // XXX1
// x.flatMap(x=> x.split(" ").toList.map(w=>w.toUpperCase())).collect
// x.map(x=> x.toUpperCase().split(" ").mkString(",")).collect
// x.map(x=> x.toUpperCase().split(" ")).flatten   //useless

//parallelize execution
//x.mapPartitions(  itr=> Iterator( itr.toList.map(x=> x.toUpperCase().split(" ").mkString(",")))).collect()
//x.mapPartitions(  itr=> Iterator( itr.toList.flatMap(x=> x.toUpperCase()) ) .collect()  //useless


println("delimite the single  Rows Array/List into multiple Rows using --Explode")
   //  Example 1
   // pick XXX1 
   // var str2Array = x.mapPartitions(  itr=> Iterator( itr.toList.flatMap(_.toUpperCase().split(" ")))).toDF("col1")
   // str2Array.select(explode(str2Array("col1"))).show()

   // Example 2
   // val expdf = sc.parallelize(Seq((1, Seq(2,3,4), Seq(5,6,7)), (2, Seq(3,4,5), Seq(6,7,8)), (3, Seq(4,5,6), Seq(7,8,9)))).toDF("a", "b", "c")
   // expdf.select(expdf("a"),expdf("b"),expdf("c")).show()
   // expdf.select(expdf("a"),explode(expdf("b")),expdf("c")).show()   // remember only one explode operation is allowed per DataFrame
   // var newdf= expdf.select(expdf("a"),explode(expdf("b")).alias("b_splits"),expdf("c"))
   // newdf.show()  // observer alias added for new derived columns
   //  newdf.select(newdf("a"),newdf("b_splits"),explode(newdf("c")).alias("c_splits")  ).show()   

  
   println("This example is using explode + is usefull  RDD to DataFrame conversion and  write/read  dataframe in parquet file")
   // Example 3
   /*
   case class emp(name: String, age:Int, email: String)    // tuple3 of String,Int,String
   case class dep(id: Int, dname: String)    // tuple2   of Int and String
   case class emp_dep(d: dep, e:Seq[emp])    // tuple2  of department(tuple2)  and employ(tuple3)
   
   var E21 = new emp("E21",21,"E21@g.com")  
   var F22 = new emp("F22",22,"F22@g.com")


   var E31 = new emp("E31",31,"E31@g.com")
   var F32 = new emp("F32",32,"F32@g.com")
   var G33 = new emp("G33",33,"G33@g.com")
  


   var E41 = new emp("E41",41,"E41@g.com")
   var F42 = new emp("F42",42,"F42@g.com")
   var G43 = new emp("G43",43,"G43@g.com")
   var H44 = new emp("H44",44,"H44@g.com")
   var I45 = new emp("I45",45,"I45@g.com")

   var D20 = new dep(20,"D20")
   var D30 = new dep(30,"D30")
   var D40 = new dep(40,"D30")

   var emp_dep_data20 = new emp_dep (D20 , Seq(E21,F22) )
   var emp_dep_data30 = new emp_dep (D30 , Seq(E31,F32,G33) )
   var emp_dep_data40 = new emp_dep (D30 , Seq(E41,F42,G43,H44,I45) )
   
   println(E21.name,E21.age,E21.email)
   println(D30.id,D30.dname)
  
   println(emp_dep_data20.d.dname,emp_dep_data20.d.id )  // observe d is input to emp_dep20 instance
   println(emp_dep_data20.e.length, emp_dep_data20.e(0),emp_dep_data20.e(1))   // observe  observe e is input to emp_dep20 instance and  e(0)/(1) is the Seq data 


  val RDDemp_dep_data20 = sc.parallelize(Seq(emp_dep_data20)).toDF // 
  val RDDemp_dep_data30 = sc.parallelize(Seq(emp_dep_data30)).toDF // 
  val RDDemp_dep_data40 = sc.parallelize(Seq(emp_dep_data40)).toDF // 


  RDDemp_dep_data20.write.parquet("RDDemp_dep_data20.parquet")
  RDDemp_dep_data30.write.parquet("RDDemp_dep_data30.parquet")
  RDDemp_dep_data40.write.parquet("RDDemp_dep_data40.parquet")


  val RDDemp_dep_data20_parque_read = spark.read.parquet("RDDemp_dep_data20.parquet")
  val RDDemp_dep_data30_parque_read = spark.read.parquet("RDDemp_dep_data30.parquet")
  val RDDemp_dep_data40_parque_read = spark.read.parquet("RDDemp_dep_data40.parquet")

  RDDemp_dep_data20_parque_read.show()
  RDDemp_dep_data30_parque_read.show()
  RDDemp_dep_data40_parque_read.show()

 import org.apache.spark.sql.Row   // remember imported Row
 
  RDDemp_dep_data20_parque_read.saveAsTextFile()

	val RDDemp_dep_data20_parque_read_explode = RDDemp_dep_data20_parque_read.explode(RDDemp_dep_data20_parque_read("e")) { 

	  case Row(employee: Seq[Row]) => employee.map(employee => 

		emp(employee(0).asInstanceOf[String], employee(1).asInstanceOf[Int], employee(2).asInstanceOf[String])

   RDDemp_dep_data20_parque_read_explode.createOrReplaceTempView("parqueview")

	) 
	}

  val sqlContext= new org.apache.spark.sql.SQLContext(sc)
  import sqlContext.implicits._

  */


println("group bykey  vs  reduceByKey ( alongwith mapPartitions)")
/*
var gupby=List((3922774869L,10,1),(3922774869L,11,1),(3922774869L,12,2),(3922774869L,13,2),(1779744180L,10,1),(1779744180L,11,1),(3922774869L,14,3),(3922774869L,15,2),(1779744180L,16,1),(3922774869L,12,1),(3922774869L,13,1),(1779744180L,14,1),(1779744180L,15,1),(1779744180L,16,1),(3922774869L,14,2),(3922774869L,15,1),(1779744180L,16,1),(1779744180L,17,1),(3922774869L,16,4))

var gupbyrdd  = sc.parallelize(gupby,8)

var rdd = gupbyrdd.map{ case (key,v1,v2) => (key,(v1,v2))}

var rdd = gupbyrdd.map{ case (key,v1,v2) => (key,(v1,v2))}

val initialSet = collection.mutable.HashSet.empty[(Int, Int)]
val addToSet = (s: collection.mutable.HashSet[(Int, Int)], v: (Int, Int)) => s += v
val mergePartitionSets = (p1: collection.mutable.HashSet[(Int, Int)], p2: collection.mutable.HashSet[(Int, Int)]) => p1 ++= p2

operator 1
val uniqueByKey = rdd.aggregateByKey(initialSet)(addToSet, mergePartitionSets)

 aggreateByKey Ouput =   Array(
(1779744180,Set((14,1), (11,1), (10,1), (15,1), (17,1), (16,1))), 
(3922774869,Set((11,1), (14,2), (13,1), (12,2), (12,1), (10,1), (13,2), (15,1), (16,4), (14,3), (15,2)))
)

operation 2
gupbyrdd.map{ case (key,v1,v2) => (key,(v1,v2))}.groupByKey().collect()

group by key output Array(
(1779744180,CompactBuffer((10,1), (11,1), (16,1), (14,1), (15,1), (16,1), (16,1), (17,1))), 
(3922774869,CompactBuffer((10,1), (11,1), (12,2), (13,2), (14,3), (15,2), (12,1), (13,1), (14,2), (15,1), (16,4)))
)

operation 3
gupbyrdd.map{ case (key,v1,v2) => ((key,v1),v2)}.reduceByKey(_+_).sortByKey().collect()

reduce By output  Array((
Array((
(1779744180,10),1), ((1779744180,11),1), ((1779744180,14),1), ((1779744180,15),1), ((1779744180,16),3), ((1779744180,17),1), 

((3922774869,10),1), ((3922774869,11),1), ((3922774869,12),3), ((3922774869,13),3), ((3922774869,14),5), ((3922774869,15),3), ((3922774869,16),4))


*/
 

defaults write com.apple.finder _FXShowPosixPathInTitle -bool true; killall Finder
defaults write com.apple.finder _FXShowPosixPathInTitle -bool false; killall Finder


def mapPartitionsWithIndex[U](f: (Int, Iterator[T]) ⇒ Iterator[U], preservesPartitioning: Boolean = false)
(implicit arg0: ClassTag[U]): RDD[U]



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


	//find the correlation 
		import org.apache.spark.mllib.linalg._
		import org.apache.spark.mllib.stat.Statistics
		import org.apache.spark.rdd.RDD

		val seriesX: RDD[Double] = sc.parallelize(Array(1, 2, 3, 3, 5))  // a series
		// must have the same number of partitions and cardinality as seriesX
		val seriesY: RDD[Double] = sc.parallelize(Array(11, 22, 33, 33, 555))

		// compute the correlation using Pearson's method. Enter "spearman" for Spearman's method. If a
		// method is not specified, Pearson's method will be used by default.
		val correlation: Double = Statistics.corr(seriesX, seriesY, "pearson")
		println(s"Correlation is: $correlation")

		val seriesY: RDD[Double] = sc.parallelize(Array(555,33,33,22,11))
		val correlation: Double = Statistics.corr(seriesX, seriesY, "pearson")



}
}




