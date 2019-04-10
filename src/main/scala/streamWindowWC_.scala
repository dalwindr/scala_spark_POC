object streamWindowWC{

def main(args: Array[String]) {

   import org.apache.spark._
   import org.apache.spark.streaming._
   import org.apache.spark.streaming.{Seconds, StreamingContext}

   println("""

    Describe : This programing is about the usage DStream(ReduceBykeyAndWindow  Function)


   	Thanks.""")

  var dir_path_cp="file:///Users/keeratjohar2305/Downloads/59e8f3c42ef0ee849a77-ef2360e85067356b16ebd3af2689db720a47963d/streamWindowWC_CP"

  def functionToCreateContext(): StreamingContext = {
    // Create a local StreamingContext with batch interval of 3 second  FOR THE FIRST TEAM
      val conf = new SparkConf().setAppName("NetworkWordCount").setMaster("local[*]")
      val ssc = new StreamingContext(conf, Seconds(3))   // new context

      val sc = ssc.sparkContext    // created just to reduce logging
      sc.setLogLevel("ERROR")


      ssc.checkpoint(dir_path_cp) 
      ssc


   }

  // CREATE STREAM CONTEXT IF NOT EXIST IN THE checkpoint
   val sscc = StreamingContext.getOrCreate(dir_path_cp, ()=>functionToCreateContext())
   
   var lines = sscc.socketTextStream("localhost",9999)
   lines.flatMap(x=> x.split(" ")).map(w=> (w,1)).reduceByKey((p,n)=> p + n ).print()
   lines.flatMap(x=> x.split(" ")).map(w=> (w,1)).reduceByKeyAndWindow((p:Int,n:Int)=> p + n , Seconds(9),Seconds(3)).print()

   
   
   sscc.start()
   sscc.awaitTermination()

}


}