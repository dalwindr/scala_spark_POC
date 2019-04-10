import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Seconds, StreamingContext, Time}
import org.apache.spark.sql.types.{StructType, StructField, StringType, IntegerType};

//var mys2= StructType("c1 c2 c3".split(" ").map(fieldName => StructField(fieldName,StringType,true)))
//var dd1 = sc.parallelize(Seq(Row(2,3,4),Row(4,5,5)))
//var o1 = spark.sqlContext.applySchema(dd1,mys2)

import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.{Connection,ConnectionFactory,HBaseAdmin,HTable,Put,Get}
import org.apache.hadoop.hbase.util.Bytes


object SparkSessionSingleton {

  @transient  private var instance: SparkSession = _

  def getInstance(sparkConf: SparkConf): SparkSession = {
    if (instance == null) {
      instance = SparkSession
        .builder
        .config(sparkConf)
        .getOrCreate()
    }
    instance
  }
}



object Dstream1{
def main(args: Array[String]) {


   println("""

    Describe : This programing is about the usage DStream(Transform  Function)
               Also added toDF trasformation with scheam via Seq


   	Thanks.""")
   
   var dir_path_opp="file:///Users/keeratjohar2305/Downloads/59e8f3c42ef0ee849a77-ef2360e85067356b16ebd3af2689db720a47963d/SStream_EX2"
   var dir_path="file:///Users/keeratjohar2305/Downloads/59e8f3c42ef0ee849a77-ef2360e85067356b16ebd3af2689db720a47963d/SIKANDRABAD"




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

      var transformWith= "file:///Users/keeratjohar2305/Downloads/59e8f3c42ef0ee849a77-ef2360e85067356b16ebd3af2689db720a47963d/Source_RDD.txt"
      
      // reading dimestion  (PART 1)
      var transformWith_data = sscc.sparkContext.textFile(transformWith)
    

      case class mys (Date : String, Narration: String, Value_Dat: String,Debit_Amount: String, Credit_Amount: String, Chq_Or_Ref_Number: String, Closing_Balance: String )
      
      var transformWith_data1 = transformWith_data.map(x=> x.split("\n")).filter(x=> !x.contains("")).flatMap(x=>x.map(x=>x.split(" ,")     ))
    
      var transformWith_data2 = transformWith_data1.map(x => ( mys(x(0).trim, x(1).trim, x(2).trim,x(3).trim,x(4).trim,x(5).trim,if (x.length==7) x(6).trim else "0.0" ))).filter(x=> !x.Date.contains("Date")).repartition(4)

      var transformWith_data3 = transformWith_data2.filter(x=> (x.Credit_Amount).toDouble > 0.0).map(x=> (x.Chq_Or_Ref_Number,(x.Credit_Amount,x.Narration ,x.Date))  )    
 
 	  transformWith_data3.distinct().foreach(x=>println (x))


      // Files Streaming (PART 2)
      var Dstream_d= sscc.textFileStream(directory = dir_path)

      var Dstream_d1=Dstream_d.map(x=> x.split("\n")).filter(x=> !x.contains("")).flatMap(x=>x.map(x=>x.split(" ,")     ))
    
       // SCHEMA IS NOT SPECIFIED VIA CASE CLASS   
      var Dstream_d3 = Dstream_d1.map(x => ( mys(x(0).trim, x(1).trim, x(2).trim,x(3).trim,x(4).trim,x(5).trim,if (x.length==7) x(6).trim else "0.0" ))).filter(x=> !x.Date.contains("Date")).repartition(4)
      
      var Dstream_d4 = Dstream_d3.filter(x=> (x.Credit_Amount).toDouble > 0.0).map(x=> (x.Chq_Or_Ref_Number,(x.Credit_Amount,x.Narration ,x.Date))  )     	  
      
     var Dstream_join = Dstream_d4.transform(rdd=> rdd.join(transformWith_data3))
      Dstream_d.print()
              
 
     // SCHEMA IS NOT SPECIFIED VIA CASE CLASS      (PART3 )
     var Dstream_d2 = Dstream_d1.map(x => ( x(0).trim, x(1).trim, x(2).trim,x(3).trim,x(4).trim,x(5).trim,if (x.length==7) x(6).trim else "0.0" )).filter(x=> !(x._1).contains("Date")).repartition(4)

      var myschema = Seq("Date","Narration", "Value_Dat","Debit_Amount","Credit_Amount","Chq_Or_Ref_Number","Closing_Balance")

      // SCHEMA IS DEFINED FOR DATAFRAME VIA SCHEMA DEFINED AS Seq OBJECT
      Dstream_d2.foreachRDD{ 
                            rdd =>
								//val spark = SparkSession.builder.config(rdd.sparkContext.getConf).getOrCreate()
								val spark = SparkSessionSingleton.getInstance(rdd.sparkContext.getConf)
  								 import spark.implicits._
	                             val mydf = rdd.toDF (myschema: _*)
								mydf.createOrReplaceTempView("test")
                               spark.sql("Select Chq_Or_Ref_Number,sum(Debit_Amount),sum(Credit_Amount) from test group by Chq_Or_Ref_Number").show() 
           }
 

      sscc.start()
      sscc.awaitTermination()
}

}