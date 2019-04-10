
   import org.apache.spark._
   import org.apache.spark.streaming._
   import org.apache.spark.streaming.{Seconds, StreamingContext}



import com.codahale.metrics.{Gauge, MetricRegistry}

import org.apache.spark.SparkContext
import org.apache.spark.annotation.Experimental
import org.apache.spark.util.{AccumulatorV2, DoubleAccumulator, LongAccumulator}


import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql
import org.apache.spark.sql.types._
//import org.apache.spark.sql
import org.apache.spark.sql.functions._ 
import org.apache.spark.SparkContext._ 
//import spark.implicits._       

//import org.apache.spark.metrics.source.{DoubleAccumulatorSource, LongAccumulatorSource}
import org.apache.spark.util.{AccumulatorV2, DoubleAccumulator, LongAccumulator}
import org.apache.spark.sql.SparkSession

// Random number generator 
import java.util.Random


import org.apache.log4j.Logger
import org.apache.log4j.Level

import org.apache.spark.HashPartitioner
import org.apache.spark.Partitioner
import org.apache.spark.RangePartitioner


//log4j.rootCategory=WARN, console

    class customPartitioner ( override val  numPartitions: Int) extends Partitioner {
       override 
       def getPartition(key: Any): Int = {
        val k = key.asInstanceOf[Int]
        return (k % numPartitions)
       }

       override
       def equals(other:scala.Any):Boolean = {
        other match {
          case obj: customPartitioner => obj.numPartitions == numPartitions
          case _  => false
        }
       }

     }


   class customPartitioner1 ( override 
                  val numPartitions: Int) extends Partitioner {
        override
        def getPartition(Key:Any): Int = 
                  Key match {
                  case s:String =>  { if ( s(0).toUpper == 'S' )  1 else 0 }
                  
        }
        override
        def equals (other:Any):Boolean = {
          other.isInstanceOf[customPartitioner1]
        }
        override
        def hashCode: Int = 0
   }


object AccumulatorMetricsTest{
def main(args: Array[String]) {

   //////import org.apache.log4j.{Level, Logger}
  //////Logger.getLogger("org").setLevel(Level.OFF)
  //////Logger.getLogger("akka").setLevel(Level.OFF)

  
   // 
  
   import org.apache.spark._
   import org.apache.spark.streaming._
   import org.apache.spark.streaming.{Seconds, StreamingContext}


   var dir_path_opp="file:///Users/keeratjohar2305/Downloads/59e8f3c42ef0ee849a77-ef2360e85067356b16ebd3af2689db720a47963d/SIKANDRABAD_CP_TEST"

   def functionToCreateContext(): StreamingContext = {
    // Create a local StreamingContext with batch interval of 3 second  FOR THE FIRST TEAM
      val conf = new SparkConf().setAppName("NetworkWordCount").setMaster("local[*]")
      val ssc = new StreamingContext(conf, Seconds(9))   // new context

      val sc = ssc.sparkContext    // created just to reduce logging
      sc.setLogLevel("ERROR")


      ssc.checkpoint(dir_path_opp) 
      ssc


   }

  // CREATE STREAM CONTEXT IF NOT EXIST IN THE checkpoint
  val sscc = StreamingContext.getOrCreate(dir_path_opp, ()=>functionToCreateContext())

  
  // Word count state function 
   def updatefunc(v: Seq[Int], rc: Option[Int]) = {
         val nc = v.sum + rc.getOrElse(0)
         new Some(nc)
      }

  // HDFC bank total credit and max credit state from My HDFC bank monthly statement file which i am copying manually to center location
def getstateofMaxcredit(newdata : Seq[(Double, Double)], running_credit_val: Option[(Double, Double)]  ) =  {
      println (" First line Inside Stae Function existing data =", running_credit_val,"   current data =", newdata)
      //var maxCredit: Some[(Double, Double)] = Some((967824.0198,9389.0))
      //var maxCredit_temp = running_credit_val.getOrElse(0.0,0.0)
      var newdata1 = newdata
      if (newdata.isEmpty )
          { 
            println("is Not empty setting default",newdata.isEmpty)
            newdata1  = Seq((0.0,0.0))
          }
      var existing_Data_tot_Credit = running_credit_val.map(x=> x._1.toDouble).getOrElse(0.0)
      var existing_Data_max_Credit = running_credit_val.map(x=> x._2.toDouble).getOrElse(0.0)
      var maxCredit = (newdata1(0)._1 + existing_Data_tot_Credit, existing_Data_max_Credit + newdata1(0)._2)


       println (" ....Last line Inside Stae Function existing data =", maxCredit,"   current data =", newdata)   
       new Some(maxCredit)
      }
      
      
  // Exp 1 (socket streaming)
   val lines = sscc.socketTextStream("localhost",9999)

   val words = lines.flatMap(_.split(" "))

   val wordCounts = words.map(x => (x, 1)).reduceByKey(_ + _)

   var tolcnt= wordCounts.updateStateByKey(updatefunc _)

   //tolcnt.print()

  
  // Exp 2 (socket streaming)
   val lines1 = sscc.socketTextStream("localhost",6666)

   val words1 = lines1.flatMap(_.split(" "))

   val wordCounts1 = words1.map(x => (x, 1)).reduceByKey(_ + _)

   var tolcnt1= wordCounts1.updateStateByKey(updatefunc _)

   //tolcnt1.print()



  // Exp 1 (file streaming)
    var dir_path="file:///Users/keeratjohar2305/Downloads/59e8f3c42ef0ee849a77-ef2360e85067356b16ebd3af2689db720a47963d/SIKANDRABAD"

    val data = sscc.textFileStream(directory = dir_path)

    case class mys (Date : String, Narration: String, Value_Dat: String,Debit_Amount: String, Credit_Amount: String, Chq_Or_Ref_Number: String, Closing_Balance: String )
    
    var mydata1=data.map(x=> x.split("\n")).filter(x=> !x.contains("")).flatMap(x=>x.map(x=>x.split(" ,")     ))
    
    var mydata2 = mydata1.map(x => ( mys(x(0).trim, x(1).trim, x(2).trim,x(3).trim,x(4).trim,x(5).trim,if (x.length==7) x(6).trim else "0.0" ))).filter(x=> !x.Date.contains("Date")).repartition(4)

    var count_tot_credit_transaction_perbatch_Interval= mydata2.map(x=> (1,x.Credit_Amount.toFloat)).countByValue()
    
    //count_tot_credit_transaction_perbatch_Interval.print()

    var max_credit= mydata2.map(x=> (1,(x.Credit_Amount.toDouble,x.Credit_Amount.toDouble))).reduceByKey((x,y) => ( x._1 + y._1 , Math.max(x._2 , y._2) )) //.map(x=> x._2)
    //println ("New data Before  state call ")

    //max_credit.print()
  
    var max_credit1= max_credit.updateStateByKey(getstateofMaxcredit _)

    //println ("New data AFTER  state call ")
    max_credit1.print()

    
    
    sscc.start()
    sscc.awaitTermination()    // add it if you want to program to run infinitaly.



/*
   import org.apache.spark._
   import org.apache.spark.streaming._
   import org.apache.spark.streaming.{Seconds, StreamingContext}

val conf = new SparkConf().setAppName("NetworkWordCount").setMaster("local[*]")
val ssc = new StreamingContext(sc, Seconds(10))


import scala.collection._
import scala.collection.mutable.Queue
import org.apache.spark.rdd.RDD
val rddQueue = new Queue[RDD[Int]]


val rdd1 = ssc.sparkContext.parallelize(Array(1,2,3))
val rdd2 = ssc.sparkContext.parallelize(Array(4,5,6))]
rddQueue.enqueue(rdd1)
rddQueue.enqueue(rdd2)

val numsDStream = ssc.queueStream(rddQueue, true)
val plusOneDStream = numsDStream.map(x => x+1)
plusOneDStream.print()

val rdd1 = ssc.sparkContext.parallelize(1 to 20)
val rdd3 = ssc.sparkContext.parallelize(100 to 105)
rddQueue.enqueue(rdd3)
rddQueue.enqueue(rdd2)


*/

   
 /*

 val ssc = new StreamingContext(sc, Seconds(15))
   // example 1 for Spartk Streaming ( TCP socket)
   // getting it working from TCP socket from reading stread via nc -lk 9999 command  ( perfect opened in different console)
   
   /* Create a DStream that will connect to hostname and port, like localhost 9999. As stated earlier, DStream will get created from StreamContext,
   // which in return is created from SparkConf ( JVM). */
   val lines = ssc.socketTextStream("localhost",9999)
  
    // Using this DStream (lines) we will perform  transformation or output operation.
    val words = lines.flatMap(_.split(" "))
    val wordCounts = words.map(x => (x, 1)).reduceByKey(_ + _)
    wordCounts.print()
  
   //To start the processing after all the transformations have been setup, we finally call
    ssc.start()       
    ssc.awaitTermination()  // Wait for the computation to terminate
*/


    // Example 2 for DAtastream Filestream
    // var dir_path="file:///Users/keeratjohar2305/Downloads/59e8f3c42ef0ee849a77-ef2360e85067356b16ebd3af2689db720a47963d/SIKANDRABAD"
    //hdfs:///user/data
    //val lines = ssc.textFileStream(dir_path)
    // textFileStream  //
   // val words = lines.flatMap(_.split(" "))
    //val wordCounts = words.map(x => (x, 1)).reduceByKey(_ + _)
   // wordCounts.print()
   // println ("counts of word in a line ")

   // ssc.start()       
   // ssc.awaitTermination()  // Wait for the computation to terminate
    
     //ssc.stop()
/*

	  //val conf = new SparkConf().setAppName("test").setMaster("local[*]")
	  //val sc = new SparkContext(conf)
    //sc.setLogLevel("ERROR")


    import org.apache.spark.sql.SparkSession    
    
   
    val spark = SparkSession.builder.appName("StructuredNetworkWordCount").getOrCreate()  
    
    import spark.implicits._
    var sc = spark.sparkContext
    sc.setLogLevel("ERROR")
    
    


    //spark.readStream.text("/path/to/directory/")  set folder path
    var fname="/Users/keeratjohar2305/Downloads/59e8f3c42ef0ee849a77-ef2360e85067356b16ebd3af2689db720a47963d/SIKANDRABAD"
    var output="/Users/keeratjohar2305/Downloads/59e8f3c42ef0ee849a77-ef2360e85067356b16ebd3af2689db720a47963d/SIKANDRABAD/output"
    val lines = spark.readStream.text(fname) 

    //example 1  ( printing  console)
    var q = lines.writeStream.format("console").start()
    q.awaitTermination()

*/

//*/

   //  example 2 
   // lines.writeStream.format(“memory”).queryName(“table1”).start()
   //spark.sql("select * from t1").show()



////spark.readStream.format("kafka").option("subscribe", "test").option("kafka.bootstrap.servers", "localhost:9092").load()

////val lines = spark.readStream.format("kafka").option("kafka.bootstrap.server", "localhost:9092").option("subscribe", "test").load()


/*
lines.foreach(x=> println ("Data coming from kafka = " , x))
    

    var number = (0 to 200)
    var list = sc.parallelize((0 to 200))


    //var data = list.take(20).foreach(println) 

    //list.take(20).toArray.foreach(println)

    //data
    var count= list.count()
    var data20 = list.take(20)
    var data = list.collect()
    //list.foreach(x=> println("data is ",x) )
   
    println ("example 1 : broadcast RDD")
    val slices = if (args.length > 0) args(0).toInt else 2
    val num = if (args.length > 1) args(1).toInt else 1000000

    val arr1 = (0 until num).toArray

    for (i <- 0 until 3) {
      println(s"Iteration broadcasted $i with array size $num and partition $slices")
      println("===========")
      var startTime = System.nanoTime
      val barr = sc.broadcast(arr1)
      val observedSizes = sc.parallelize(1 to 10, slices).map(_ => barr.value.length)
      // Collect the small RDD so we can print the observed sizes locally.
      observedSizes.collect().foreach(i => println(i))
      println("Iteration broadcasted %d took %.0f milliseconds".format(i, (System.nanoTime - startTime) / 1E6))

     }
   
    println ("example 2 : random number generator")

    /* map output
    sc.parallelize(0 to 4).map{x=> 
    	  var ranGen =new Random; 
    	  var myll= new Array[(Int,Array[Byte])](3) ; 
    	  for(i<- 0 until 3 ){ 
    	  	val byteArr = new Array[Byte](3);
    	  	ranGen.nextBytes(byteArr);myll(i)=(ranGen.nextInt(Int.MaxValue),byteArr)
    	     };
    	     myll 
    	  }.collect()

			Array(Array((2072733347,Array(79, -115, 59)), (2137883106,Array(18, -108, -41)), (437199470,Array(-122, 86, 21))), 
				  Array((1880871212,Array(65, -39, -22)), (2052115386,Array(-59, 69, -80)), (556628865,Array(64, -55, -33))), 
				  Array((1661373863,Array(-81, -73, -58)), (2138444838,Array(-108, -105, -16)), (1139921301,Array(85, 6, -90))), 
				  Array((1164504855,Array(37, 40, -120)), (883375658,Array(-125, -66, 87)), (1872029588,Array(85, 95, -56))), 
				  Array((415084102,Array(-105, -75, -58)), (1338897843,Array(90, -51, 95)), (1653040394,Array(-59, -15, -14))))
    */

    /*  flatMap output
        var myx= sc.parallelize(0 until 5).flatMap{x=> 
    	  var ranGen =new Random; 
    	  var myll= new Array[(Int,Array[Byte])](5) ; 
    	  for(i<- 0 until 5 ){ 
    	  	val byteArr = new Array[Byte](3);
    	  	ranGen.nextBytes(byteArr);
    	  	myll(i)=(ranGen.nextInt(Int.MaxValue),byteArr)
    	     };
    	     myll 
    	  }.cache()




     	Array((698927693,Array(-72, 99, -69)), 
     		  (1837879705,Array(70, -25, -30)), 
     		  (995593636,Array(-7, 94, -84)), 
     		  (358929790,Array(-44, 44, -12)), 
     		  (2013476527,Array(30, -47, -125)), 
     		  (1551870183,Array(-34, -62, -12)), 
     		  (1917152059,Array(-9, 32, -21)), 
     		  (832746474,Array(-30, -68, 127)), 
     		  (2035763026,Array(46, 62, 12)), 
     		  (1575700108,Array(125, -97, 125)), 
     		  (620167518,Array(79, 121, -43)), 
     		  (689051209,Array(81, 99, -27)), 
     		  (1261704900,Array(105, -24, -76)), 
     		  (1096247360,Array(60, -18, -16)), 
     		  (1782826272,Array(14, 62, 63)), 
     		  (1990448599,Array(-14, 19, 42)), 
     		  (1169407343,Array(-96, 78, 48)), 
     		  (778536942,Array(93, -127, -15)), 
     		  (1990021453,Array(114, 34, -80)), 
     		  (547386672,Array(123, -64, 77)), 
     		  (1123207379,Array(60, -59, -111)), 
     		  (1252881279,Array(30, -57, 47)), 
     		  (1641185365,Array(-...
scala> 


    
          */


    val numMappers = if (args.length > 0) args(0).toInt else 2
    val numKVPairs = if (args.length > 1) args(1).toInt else 1000
    val valSize = if (args.length > 2) args(2).toInt else 1000
    val numReducers = if (args.length > 3) args(3).toInt else numMappers



    val pairs1 = sc.parallelize(0 until numMappers, numMappers).flatMap { p =>
      val ranGen = new Random
      val arr1 = new Array[(Int, Array[Byte])](numKVPairs)
      for (i <- 0 until numKVPairs) {
        val byteArr = new Array[Byte](valSize)
        ranGen.nextBytes(byteArr)
        arr1(i) = (ranGen.nextInt(Int.MaxValue), byteArr)
      }
      arr1
    }.cache()

    pairs1.map(x=>(x._1,x._2.toList.mkString(",")))
   
    //println(data.getClass,list.getClass,data20.getClass,count.getClass )
    println(pairs1.groupByKey(numReducers).count())

	println ("hello world")  

	println(" hash partition vs range partition vs custome partition")

	var datax = sc.parallelize(0 to 5)
	var data1 = sc.parallelize(100 to 105)
	var kvdata= datax.cartesian(data1)
	
	var hpr = new HashPartitioner(5)   // HashPartitioner  

	var rpr = new RangePartitioner(5,kvdata)  // RangePartitioner

	var hpdata = kvdata.partitionBy(hpr)
    var rpdata =  kvdata.partitionBy(rpr)

    println( "rpdata partition count",rpdata.getNumPartitions)
    println( "hpdata partition count",hpdata.getNumPartitions)
     /*
        			scala> rpdata.mapPartitionsWithIndex( (i,itr) => Iterator( (i,itr.toList) )).foreach(println)
			(0,List((0,100), (0,101), (0,102), (0,103), (0,104), (0,105), (1,100), (1,101), (1,102), (1,103), (1,104), (1,105)))
			(2,List((3,100), (3,101), (3,102), (3,103), (3,104), (3,105)))
			(1,List((2,100), (2,101), (2,102), (2,103), (2,104), (2,105)))
			(3,List((4,100), (4,101), (4,102), (4,103), (4,104), (4,105)))
			(4,List((5,100), (5,101), (5,102), (5,103), (5,104), (5,105)))

			scala> hpdata.mapPartitionsWithIndex( (i,itr) => Iterator( (i,itr.toList) )).foreach(println)
			(3,List((3,100), (3,101), (3,102), (3,103), (3,104), (3,105)))
			(1,List((1,100), (1,101), (1,102), (1,103), (1,104), (1,105)))
			(0,List((0,100), (0,101), (0,102), (0,103), (0,104), (0,105), (5,100), (5,101), (5,102), (5,103), (5,104), (5,105)))
			(2,List((2,100), (2,101), (2,102), (2,103), (2,104), (2,105)))
			(4,List((4,100), (4,101), (4,102), (4,103), (4,104), (4,105)))


      */
    
      println ("asInstanceOf vs isInstanceof  == vs equals")
      /*
      trait person
      case class emp (name : String , age: Int) extends person
	  case class stu (name : String , age: Int) extends person
	
	  var e1 = new emp ("dalwinder",44)
	  var e2 = new emp ("praveen",42)
 
 	  var s1 = new stu ("dalwinder",44)
	  var s2 = new stu ("chanchal",42)

	  // checking where the object is the instance of given class on not
	  println (e1.isInstanceOf[emp] ,e2.isInstanceOf[emp] )   //  return true 
	  println (s1.isInstanceOf[stu] ,s2.isInstanceOf[stu] )   //  return false

	  // check if object can be casted on not.
	  println (e1.asInstanceOf[emp] ,e2.asInstanceOf[emp] )   // return actual data
	  println (s1.asInstanceOf[stu] ,s2.asInstanceOf[stu] )    // return actual data

	  // super parent
	  println (s1.isInstanceOf[person] ,s2.isInstanceOf[person] )  // true
	  println (e1.isInstanceOf[person] ,e1.isInstanceOf[person] )  // true


      // neigbour object testing
	  println (s1.isInstanceOf[emp] ,s2.isInstanceOf[emp] )  // false

	 
	  println (e1 equals e1)   // true
	  println (e1 equals s1)   // false
	  println (e1 equals e2)  // false

      class emp1 (name : String , age: Int) //extends person
	  var e11 = new emp1 ("dalwinder",44)
	  var e21 = new emp1 ("dalwinder",44)


 	  Custom Partitioners
    
      class customPartitioner:			To implement a custom partitioner, we need to extend the Partitioner class and implement the three methods which are:
				
				numPartitions : Int : returns the number of partitions 
				
				getPartitions(key : Any) : Int :  returns the partition ID (0 to numPartitions-1) for the given key
				
				equals() : This is the standard Java equality method. Spark will need to test our partitioner object against other instances of itself when it decides whether two of our RDDs are partitioned the same way!!
				
				Below is the simple custom partitioner example:
2


      */


 

   var cust_dat = sc.parallelize(Array(("dalwinder",23),("Sandeep",34),("Sharma",25),("jasmeet",19))) 							
   var cpr = new customPartitioner1(2)

   var cpr_data = cust_dat.partitionBy(cpr)

   cpr_data.mapPartitionsWithIndex( (i,itr) => Iterator( (i,itr.toList) )).foreach(println)
   cpr_data.glom().collect()

*/

/*
sparkContext was used as a channel to access all spark functionality.
The spark driver program uses spark context to connect to the cluster through a resource manager (YARN orMesos..).

sparkConf is required to create the spark context object, which stores configuration parameter like appName (to identify your spark driver), application, number of core and memory size of executor running on worker node

In order to use APIs of SQL,HIVE , and Streaming, separate contexts need to be created.

like

val conf=newSparkConf()

val sc = new SparkContext(conf)

val hc = new hiveContext(sc)

val ssc = new streamingContext(sc).





*/





}

}

