// scalastyle:off println

//libraryDependencies += "io.github.hbase4s" %% "hbase4s-core" % "1.4.9"
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel
//import org.apache.spark.streaming.{Seconds, StreamingContext, Time}

import org.apache.hadoop.hbase.{HBaseConfiguration, HTableDescriptor,HColumnDescriptor,HConstants,CellUtil}
import org.apache.hadoop.hbase.client.{HBaseAdmin, Result,Put,HTable,ConnectionFactory,Connection,Get}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.util.Bytes

//import org.apache.spark.sql.datasources.hbase.{Field, HBaseTableCatalog}
//import org.apache.spark.sql.{DataFrame, SaveMode, Row, SQLContext}
//val spark = SparkSession.builder().appName("Spark HBase Example").master("local[4]").getOrCreate()
//val rdd = sc.newAPIHadoopRDD(hconf, classOf[TableInputFormat], classOf[ImmutableBytesWritable], classOf[Result])
//hconf.set(TableInputFormat.INPUT_TABLE, table)


object ApachWC {
  
  def main(args: Array[String]) {
   
    val hconf = HBaseConfiguration.create()

    val admin = new HBaseAdmin(hconf)

    //List all the tables = (multiple table descriptor)
    admin.listTables

    //table.setAutoFlush(false)


    // return list of table array
    admin.getTableNames("student")

    // return given table name in String format
    var tabName_string= admin.getTableNames("student")(0)
    val table = new HTable(hconf,tabName_string)  // create table connection

    println("row not started yet")

    admin.listTables.foreach(x=>println(x.getTableName))
    admin.listTables.foreach(x=>println(x.getColumnFamilies.toList))
    admin.listTables.foreach(x=>println(x.getFamilies))

    //for single table ( single table descriptor )
    admin.listTables("test").foreach(x=> println(x.getFamilies))
    admin.listTables("test").foreach(x=> println(x.getTableName))

    //return column famility -column Descriptor
    admin.listTables("student").map(x=> x.getFamilies)

    for (x<- (301 to 400)) {
      var p = new Put(new String("row-id" + x).getBytes())
      p.add("personal".getBytes(),"name".getBytes(),new String("Dalwinder-" +  x).getBytes())
        p.add("personal".getBytes(),"age".getBytes(),new String("" +  x).getBytes())
        p.add("personal".getBytes(),"lname".getBytes(),new String("singh-").getBytes())
        p.add("address".getBytes(),"flat/house-no".getBytes(),new String("B-" + x ).getBytes())
        p.add("address".getBytes(),"area".getBytes(),new String("Adithi Bliss Apartment, GUNJUR").getBytes())
        p.add("address".getBytes(),"pincode".getBytes(),new String("1100-"+ x).getBytes())
        p.add("address".getBytes(),"City".getBytes(), new String("Banaglore").getBytes())
        p.add("address".getBytes(),"state".getBytes(),new String("KARNATAKA-1100" + x).getBytes())
        p.add("address".getBytes(),"Country".getBytes(),new String("India").getBytes())
        p.add("study".getBytes(),"highest degree".getBytes(),new String("'Diploma in computer engineering"+ x).getBytes())
        p.add("study".getBytes(),"collega".getBytes(),new String("Guru nanak dev politechnic"+ x ).getBytes())
        p.add("study".getBytes(),"passing year".getBytes(),new String("2005"+ x).getBytes())
        table.put(p)
        println("row added")
    }


    table.flushCommits()
    println("data flushed")
    var data= table.get(new Get(Bytes.toBytes("row-id97")))
    Bytes.toString(data.getRow())
    Bytes.toString(data.getValue("personal".getBytes(),"name".getBytes()))
    data.rawCells()
    data.rawCells().foreach(x=> println(Bytes.toString(CellUtil.cloneQualifier(x))))
    data.rawCells().foreach(x=> println(Bytes.toString(CellUtil.cloneValue(x))))
    data.rawCells().foreach(x=> println(Bytes.toString(CellUtil.cloneFamily(x))))


    table.close()
    


  }
}
   //val conf = new SparkConf().setAppName("HBaseReadWrite").setMaster("local[*]")

   //val sc = new SparkContext(conf)
   //val spark = SparkSessionSingleton.
   //val spark = SparkSession.builder(conf)
   //val spark = SparkSession.builder().appName("HBaseReadWrite").getOrCreate()
/*
   val spark = SparkSession
   .builder()
   .appName("HBaseReadWrite")
   //.config("spark.sql.warehouse.dir", warehouseLocation)
   //.enableHiveSupport()
   .getOrCreate()
   */
   //val hconf = HBaseConfiguration.create()
   //conf.set("hbase.zookeeper.quorum","xxx.xxx.xxx.xxx") // xxx.xxx.xxx.xxx IP address of my Cloudera virtual machine.
//conf.set("hbase.zookeeper.property.clientPort", "2181")
   //val connection = ConnectionFactory.createConnection(conf1);
   //val admin = connection.getAdmin();

/*
   // StreamingExamples.setStreamingLogLevels()

    // Create the context with a 2 second batch size
    val sparkConf = new SparkConf().setAppName("SqlNetworkWordCount")
    val ssc = new StreamingContext(sparkConf, Seconds(2))

    // Create a socket stream on target ip:port and count the
    // words in input stream of \n delimited text (eg. generated by 'nc')
    // Note that no duplication in storage level only for running locally.
    // Replication necessary in distributed scenario for fault tolerance.
    val lines = ssc.socketTextStream(args(0), args(1).toInt, StorageLevel.MEMORY_AND_DISK_SER)
    val words = lines.flatMap(_.split(" "))

    // Convert RDDs of the words DStream to DataFrame and run SQL query
    words.foreachRDD { (rdd: RDD[String], time: Time) =>
      // Get the singleton instance of SparkSession
      val spark = SparkSessionSingleton.getInstance(rdd.sparkContext.getConf)
      import spark.implicits._

      // Convert RDD[String] to RDD[case class] to DataFrame
      val wordsDataFrame = rdd.map(w => Record(w)).toDF()

      // Creates a temporary view using the DataFrame
      wordsDataFrame.createOrReplaceTempView("words")

      // Do word count on table using SQL and print it
      val wordCountsDataFrame =
        spark.sql("select word, count(*) as total from words group by word")
      println(s"========= $time =========")
      wordCountsDataFrame.show()
    }

    ssc.start()
    ssc.awaitTermination()
  }
}


/** Case class for converting RDD to DataFrame */
case class Record(word: String)


/** Lazily instantiated singleton instance of SparkSession */
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

*/