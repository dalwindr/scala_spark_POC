

//ORC .............

usersDF.write.format("orc")
  .option("orc.bloom.filter.columns", "favorite_color")
  .option("orc.dictionary.key.threshold", "1.0")
  .save("users_with_options.orc")

val mydataframe = ... //put some data in your dataframe, friend

mydataframe
  .write
  .option("orc.compress", "snappy")
  .mode(SaveMode.Append)
  .orc("/this/is/an/hdfs/directory/")


val mydataframe = ... //put some data in your dataframe, friend  / partitioned

mydataframe
  .write
  .partitionBy("year", "month", "day", "hour")
  .option("orc.compress", "snappy")
  .mode(SaveMode.Append)
  .orc("/this/is/another/hdfs/directory")










//CSV ...........

  val peopleDFCsv = spark.read.format("csv")
  .option("sep", ";")
  .option("inferSchema", "true")
  .option("header", "true")
  .load("examples/src/main/resources/people.csv")
 




// JSON ...............
  val peopleDF = spark.read.format("json").load("examples/src/main/resources/people.json")
peopleDF.select("name", "age").write.format("parquet").save("namesAndAges.parquet")






//PARQUET ...................

 usecase -- 1) read JOSN/ write parque / read parque / create DF / do select * 
 usercase 2)  take parque file with schema ( c1 and c2) / take another parque file with column c3, c4 -- merge two file along with schema (c1,c2,c3,c4)
  				 mergedDF = spark.read.option("mergeSchema", "true").parquet("data/test_table")


 val sqlDF = spark.sql("SELECT * FROM parquet.`examples/src/main/resources/users.parquet`").toDF.select("name", "favorite_color").write.save("namesAndFavColors.parquet")
  RDDemp_dep_data20.write.parquet("RDDemp_dep_data20.parquet")
  RDDemp_dep_data30.write.parquet("RDDemp_dep_data30.parquet")
  RDDemp_dep_data40.write.parquet("RDDemp_dep_data40.parquet")


  val RDDemp_dep_data20_parque_read = spark.read.parquet("RDDemp_dep_data20.parquet")
  val RDDemp_dep_data30_parque_read = spark.read.parquet("RDDemp_dep_data30.parquet")
  val RDDemp_dep_data40_parque_read = spark.read.parquet("RDDemp_dep_data40.parquet")






//partition .....................................

peopleDF.write.bucketBy(42, "name").sortBy("age").saveAsTable("people_bucketed")
usersDF.write.partitionBy("favorite_color").format("parquet").save("namesPartByColor.parquet")


usersDF
  .write
  .partitionBy("favorite_color")
  .bucketBy(42, "name")
  .saveAsTable("users_partitioned_bucketed")


  // HIVE in spark .......................

use for spark.sql.warehouse.dir to specify hive warehouse path

import spark.implicits._
import spark.sql
import java.io.File
import org.apache.spark.sql.{Row, SaveMode, SparkSession}
import org.apache.spark.sql.hive.HiveContext

// this should look familiar
val conf = new SparkConf()
val sc = new SparkContext(conf)


val warehouseLocation = new File("spark-warehouse").getAbsolutePath

val spark = SparkSession
  .builder()
  .appName("Spark Hive Example")
  .config("spark.sql.warehouse.dir", warehouseLocation)
  .enableHiveSupport()
  .getOrCreate()

or 

   {   val hiveContext = new HiveContext(sc)   }


sql("CREATE TABLE IF NOT EXISTS src (key INT, value STRING) USING hive")
sql("LOAD DATA LOCAL INPATH 'examples/src/main/resources/kv1.txt' INTO TABLE src")

// Queries are expressed in HiveQL
sql("SELECT * FROM src").show()


// setup this fella

val rdd = ... // this is where the data goessssss

// boom shocka-locka, turn that RDD into a DF
  hiveContext.createDataFrame(rdd)
  .option("orc.compress", "snappy")
  .mode(SaveMode.Append)
  .orc("/this/is/yet/another/hdfs/directory/")

val mydstream = ... // these usually come from Spark Streaming apps





// AVRO  ........................ 

val usersDF = spark.read.format("avro").load("examples/src/main/resources/users.avro")
usersDF.select("name", "favorite_color").write.format("avro").save("namesAndFavColors.avro")


// bill generationg -- batch 
// credit card clearence