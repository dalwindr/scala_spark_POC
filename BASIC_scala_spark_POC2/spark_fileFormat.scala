
val sqlContext = new org.apache.spark.sql.SQLContext(sc)

class fileFormats {
	
def main(args: Array(String)) {

println ("hello world -- Here you see the diffrent file format theory with POC")
/*	

DF vs DS Vs RDD.
RDD = we do manuall type converssion to each column in map function.
val output = sc.textFile("/user/nsaby/testevent").map(event => event.split(",")) // split event with "," separator
                   .map(splittedEvents => (splittedEvents(1), splittedEvents(2).toInt)) // Pair the events (a tupple)
                   .reduceByKey(_+_) // Group and sum the event by key
                   .collect() // And finally collect the event

DF/ DS we create structure and inferschema.

import org.apache.spark.sql.types._
import org.apache.spark.sql.types.{StructType, StructField, StringType, IntegerType};
val customSchema = StructType(Seq(StructField("userId", StringType, true), StructField("color", StringType, true), StructField("count", IntegerType, true)))
val resultAsACsvFormat = spark.read.schema(customSchema).option("delimiter", ",").csv("/user/nsaby/testevent")
or 
val resultAsACsvFormat = spark.read.schema(inferSchema,true).option("delimiter", ",").csv("/user/nsaby/testevent")


Note: 1
Adopt the file format if
1) it is splitable in blocks, So that multiple mapper can process it.
2) it can be compressed ( bzip, bzip2, lZO, snappy) at row level and block level.
3) it can hold large info with small size.
4) it can support schema evoluation ( changes in the column and data type)
5) can read and write fast.
6) fast seaches and seek time.

There is no one file which provide all the feature and there are trade off between the above feature in the different available file formats.

File format are
		1) object file 
		2) sequance file 
		3) RC/ ORC file 
		4) parquet file 
		5) avro file 
		6) json 
		8) csv 
		9) text

Types of file format based on size
      A) Text based (TEXT / CSV / JSON / XML --> here heavies file is XML )
      Features  :   Data is TEXT, are splittable, slow to read/write , file base compression but create huge map if  splits, schema evolumn is not super tough for JSON/ XML for CSV it is possible  
      
      B) serial     (Binary sequene file / object File)
      
      C) coloumnar  (Binary ORC/PARQET/AVRO)

NoteL: 2
Type of Context suppport by Spark
1) Spark Context
2) SQL Context
3) Hive Context


Notes:   Different file formats using SparkContext – textFile, sequenceFile, object file
    	 Different file formats using SQLContext – parquet, orc, avro, json,csv
         Understand different compression algorithms – deflate, gzip, bzip2, snappy

         Since Protocol buffers & thrift are serializable but not splittable they are not largely popular on HDFS use cases and thus Avro becomes the first choice …

 
 */


/*  1) TEXT FORMAT  
	            
		has to be zipped to compress them and store them in hdfs but zip formats are not splitables , 
       To processing one spit, we need huge map task is required  

		# TEXT FORMAT 
				sparkContext.saveAsTextFile(<path to file>,classOf[compressionCodecClass]);
				sparkContext.textFile(<path to file>);

		# JSON   
				SQLContext.read.json("sample/json/")
				SQLContext.write.json("sample/json/")

		# CSV
				SQLContext.read.csv("sample/csv/", header=True, inferSchema=True)
				SQLContext.write.csv("sample/json/")

		Note:   CSV consumes less space as compared to JSON because JSON stores the data column name as addition info

		# XML
				SQLContext.read.format("xml") \
				    .options(rowTag="book").load("sample/xml/")
				SQLContext.write.format("xml") 

		Note:    if you observe care in XML we have column name with open and close value  while json as column name with open value  only.
		So XML is heavy and more space consuming the XML, 

 */

/*
Apart from text files, Spark’s Scala API also supports several other data formats:

	SparkContext.wholeTextFiles lets you read a directory containing multiple small text files, and returns each of them as (filename, content) pairs. 
This is in contrast with textFile, which would return one record per line in each file.


For other Hadoop InputFormats, you can use the SparkContext.hadoopRDD method, which takes an arbitrary JobConf and input format class, 
key class and value class. Set these the same way you would for a Hadoop job with your input source. 

You can also use SparkContext.newAPIHadoopRDD for InputFormats based on the “new” MapReduce API (org.apache.hadoop.mapreduce).


*/

/*
      2) SEQUENCE  file is  KV based ,binary format,featured with serialized and splitable (multiple mapper support)with segments and compressions property, 
      				faster read/write than text, not for hive , allow 1b block level compression or also record level compression
   					like wise text only it donnot encode schema

			 		These should be subclasses of Hadoop’s Writable API interface, like IntWritable and Text. In addition, 
			 		for example, sequenceFile[Int, String] will automatically read IntWritables and Texts.

			 		Most luring feature of Sequence file is 
			 		The sync markers in these files allow Spark to find particular point in the file and re-synchronizing it with record limits.

			 		Object files is created from Images and then coverested to MD5 values out this byte array and then compare the MD5 id to remove duplicate.

			 		this can be compressed and split into multiple blocks  and thus can be processed by multiple mappers

			 		Can generate one large sequence files from small small text file/ pdf files/ image / log file
   			
   			# WRITE
   				rdd.saveAsSequenceFile(<path location>, Some(classOf[compressionCodecClass]))
				//use any codec here (BZip2Codec,GZipCodec,SnappyCodec)			
				//here rdd is MapPartitionRDD and not the regular pair RDD.

            # READ
				sparkContext.sequenceFile(<path location>,classOf[<class name>],classOf[<compressionCodecClass >]);
				//read the head of sequence file to understand what two class names need to be used here

	  3) OBJECT FILE FEATURED WITH serialized OBJECT  While this is not as efficient as specialized formats like Avro, it offers an easy way to save any RDD.
	  		Object files are the packaging around Sequence files that enables saving RDDs containing value records only. 
	  		Saving an Object file is quite simple as it just requires calling saveAsObjectFile() on RDD.

	  		object file uses java serialisation but better is kryo serialisation.
	            
	            # WRITE 
	                RDD.saveAsObjectFile and 
                # READ
	                SparkContext.objectFile support saving an RDD in a simple format consisting of serialized Java objects. 



      4) RC and ORC is the columnar binary files offer 75 % more compression ( block compression 256 MB) than RC ( block compression 4 MB) 
		    and way more compression then sequence and textfile
		    RC  create first ROW split and then make it columnar and but ORC donnot create row splits and it group rows in stripes and ORC has lightwight index
		    RC nead MetaStore for schema store like DATABASE tables other hand ORC has encoder in it to support inline schema Store  and it support min,max,count,sum

			# WRITE
				df.write.mode(SaveMode.Overwrite).format(“orc”) .save(<path to location>)
			# READ
				sqlContext.read.orc(<path to location>); //this results in a dataframe


*/

//var sSession = org.apache.spark.sql.SparkSession.getOrCreate();
//var sContext = sSession.sparkContext;

println("JSON SINGLE LINE FILE PROCESSING with spark.read formatter")


var customer_json_fname = "/Users/keeratjohar2305/Downloads/59e8f3c42ef0ee849a77-ef2360e85067356b16ebd3af2689db720a47963d/customer_json"
var ex1_json_fname = "/Users/keeratjohar2305/Downloads/59e8f3c42ef0ee849a77-ef2360e85067356b16ebd3af2689db720a47963d/example_1.json"


val customer_json = sqlContext.jsonFile(customer_json_fname)
val ex1_json = sqlContext.jsonFile(customer_json_fname)


customer_json.registerTempTable("customers")
ex1_json.registerTempTable("ex1_data")


sqlContext.sql("SELECT first_name, address.city, address.state FROM customers").show()
sqlContext.sql("SELECT * FROM ex1_data limit 10").show()


val firstCityState = sqlContext.sql("SELECT first_name, address.city, address.state FROM customers limit 10")
val ex1_data_n = sqlContext.sql("SELECT * FROM ex1_data")

// firstCityState.collect.foreach(println)
// ex1_data_n.collect.foreach(println)





println("JSON multiline DATASET PROCESSING with spark.read.option manually")

var json1_Data = """{"id" : "1201", "name" : "satish", "age" : "25"}
{"id" : "1202", "name" : "krishna", "age" : "28"}
{"id" : "1203", "name" : "amith", "age" : "39"}
{"id" : "1204", "name" : "javed", "age" : "23"}
{"id" : "1205", "name" : "prudvi", "age" : "23"}"""

var json1_Data_rdd = sc.parallelize(json1_Data.split("\n"))    // split the data and create rdd
json1_Data_rdd.foreach(println)

var json1_data_ready = sqlContext.read.option("multiline","true").json(json1_Data_rdd)  // covert the multiline json to Row
json1_data_ready.show   // display row
json1_data_ready.printSchema  // display schema
json1_data_ready.collect().foreach(println)

json1_data_ready.createOrReplaceTempView("json1_data_view")   // create view
spark.sql("select * from json1_data_view").show()  // show view data


println("JSON multiline FILE PROCESSING with spark.read.option..........................")

var baby_names_json = "/Users/keeratjohar2305/Downloads/59e8f3c42ef0ee849a77-ef2360e85067356b16ebd3af2689db720a47963d/baby-names.json"  // filename
val baby_names_Multiline_rdd = spark.read.option("multiline", "true").json(baby_names_json)  //  read file and create rdd ..you can split into steps
baby_names_Multiline_text.show()
baby_names_Multiline_text.printSchema
baby_names_Multiline_text.select(baby_names_Multiline_text("Count")<= 219).show() // select with where
baby_names_Multiline_text.filter(baby_names_Multiline_text("Count")<= 219).show()  // filter with where
baby_names_Multiline_text.select("First Name").show()
baby_names_Multiline_text.collect().printSchema


baby_names_Multiline_text.createOrReplaceTempView("baby_names")
sqlContext.sql("SELECT * from baby_names").show()

// json to text/csv/orc/parquet/json is possible with append and overwrite mode
baby_names_Multiline_text.write.mode("append").orc("baby_names_Multiline.orc")  // true
baby_names_Multiline_text.write.csv("baby_names_Multiline.csv")  // true
baby_names_Multiline_text.write.mode("append").json("baby_names_Multiline.json")  // true
baby_names_Multiline_text.write.mode("append").csv("baby_names_Multiline.csv") // true
baby_names_Multiline_text.select("count","year").write.format("parquet").save("baby_names_Multiline.parquet")  // true
baby_names_Multiline_text.select("count","year").write.format("parquet").mode("overwrite").save("baby_names_Multiline.parquet")  // true
baby_names_Multiline_text.write.format("text").save("baby_names_Multiline.text")  // true
baby_names_Multiline_text.select("count").write.format("text").mode("overwrite").save("baby_names_Multiline.text")   // single column allowed



var r1 = sc.parallelize(List.range(1,5))
var result = sc.parallelize(List(1,2,3,4,5,6,3,4,3,2,555,64,3,3344))
val un=result.cartesian(r1)


un.saveAsObjectFile("saveAsObjectFile")
un.saveAsTextFile("saveAsTextFile")       
un.saveAsSequenceFile("saveAsSequenceFile")
un.write.orc("un.orc") // fail
un.write.csv("un.csv") //fail
un.write.json("un.json") //fail



println("CSV FILEFORMAT with SPARK TEXTFILE and String Reader  with CASE CLASS -        Example")


import java.io.StringWriter
var train_csv = "/Users/keeratjohar2305/Downloads/59e8f3c42ef0ee849a77-ef2360e85067356b16ebd3af2689db720a47963d/train.csv"
var HistoricalQuotes = "/Users/keeratjohar2305/Downloads/59e8f3c42ef0ee849a77-ef2360e85067356b16ebd3af2689db720a47963d/HistoricalQuotes.csv"

//import java.io.StringReader
//import au.com.bytecode.opencsv.CSVReader

//import java.io.StringWriter
//import au.com.bytecode.opencsv.CSVWriter;
//val stringWriter = new StringWriter
//new csvWriter = new CSVWriter(stringWriter)

import java.io.StringReader
import au.com.bytecode.opencsv.CSVReader

var train_csv = "/Users/keeratjohar2305/Downloads/59e8f3c42ef0ee849a77-ef2360e85067356b16ebd3af2689db720a47963d/train.csv"
val train_rdd = sc.textFile(train_csv)
var full_train_data  = train_rdd.map{line =>  var csvReader = new CSVReader(new StringReader(line)) ; csvReader.readNext();  }
full_train_data.collect()


type i = int
type s = String
case class trainSchema (Loan_ID :s ,Gender :s, Married :s, Dependents :s,Education :s,Self_Employed :s,ApplicantIncome :s,CoapplicantIncome :s,
	LoanAmount :s,Loan_Amount_Term :s, Credit_History :s, Property_Area :s,Loan_Status :s)


var full_train_data_with_schema = full_train_data.mapPartitionsWithIndex{(idx,itr)=> if (idx==0) itr.drop(1); 
	                 itr.toList.map(x=> trainSchema(x(0),x(1),x(2),x(3),x(4),x(5),x(6),x(7),x(8),x(9),x(10),x(11),x(12))).iterator }.toDF



/*
var full_train_data_with_schema = full_train_data.map(x=> trainSchema(x(0),x(1),x(2),x(3),x(4),x(5),x(6),x(7),x(8),x(9),x(10),x(11),x(12)))                                                                                                                                                                
full_train_data_with_schema.map(x=> Array(x.Loan_ID,x.Loan_Status,x.Dependents,x.Self_Employed,x.Gender).toArray).mapPartitions{itr => 
val stringWriter = new StringWriter ;
val csvWriter = new CSVWriter(stringWriter);
csvWriter.writeAll(itr.toList);
Iterator(stringWriter.toString.trim)
 }.saveAsTextFile("full_train_data_with_schema")   // Not working
*/

println("CSV FORMAT PROCESSING WITH SPARK.formatter   -        Example 2") 



var train_csv = "/Users/keeratjohar2305/Downloads/59e8f3c42ef0ee849a77-ef2360e85067356b16ebd3af2689db720a47963d/train.csv"
val train_rdd1 =spark.read.format("csv").option("header","true").load(train_csv) 
train_rdd1.createOrReplaceTempView("train_rdd1_view")
var train_rdd2= spark.sql("select Loan_ID,Married,Education,ApplicantIncome from train_rdd1_view where Gender ='Male' limit 15")

train_rdd2.

train_rdd2.write.mode("append").orc("train_rdd2.orc")  // true
train_rdd2.write.csv("train_rdd2.csv")  // true
train_rdd2.write.mode("append").json("train_rdd2.json")  // true
train_rdd2.write.mode("append").csv("train_rdd2.csv") // true

train_rdd2.write.format("parquet").save("train_rdd2.parquet")  // true
train_rdd2.write.format("parquet").mode("overwrite").save("train_rdd2.parquet")  // true
train_rdd2.write.format("parquet").mode("append").save("train_rdd2.parquet")  // true



var HistoricalQuotes_csv = "/Users/keeratjohar2305/Downloads/59e8f3c42ef0ee849a77-ef2360e85067356b16ebd3af2689db720a47963d/HistoricalQuotes.csv"
val historicalQuotes_rdd =  sc.textFile(HistoricalQuotes_csv)
var full_hist_quotes= historicalQuotes_rdd.map{ line => val csvReader = new CSVReader( new StringReader(line) ) ; csvReader.readNext();  } 
full_hist_quotes.collect()



var selected_train_data  = train_rdd.map{line =>  
		var csvReader = new CSVReader(new StringReader(line)) ; 
		csvReader.readAll().map(x => Stocks(x(0), x(6)))
		 }



//train_rdd2.saveAsObjectFile("train_rdd2.saveAsObjectFile")
//train_rdd2.saveAsTextFile("train_rdd2.saveAsTextFile")       
//train_rdd2.saveAsSequenceFile("train_rdd2.saveAsSequenceFile")

/*

val dataRDD = sc.textFile(“data/departments”)
dataRDD.saveAsObjectFile(“data/departmentsObj”)
val data = sc.objectFile(“data/departmentsObj”)
data.collect().foreach(println)
:21: error: value foreach is not a member of Array[Nothing]
data.collect().foreach(println)
*/

/*

    val sc = new SparkContext(new SparkConf().setAppName("Spark-Sequence-Files").setMaster("local[1]"))

    val data = sc.textFile("file:////data/Spark/spark-scala/src/main/resources/olympics_data.txt")

    data.map(x => x.split(",")).map(x => (x(1).toString(), x(2).toString())).foreach(f => print(f))

    val pairs: RDD[(String, String)] = data.map(x => x.split(",")).map(x => (x(1).toString(), x(2).toString()))

    pairs.saveAsSequenceFile("/data/spark/rdd_to_seq")

*/

  
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


//show databases in Hive
sqlContext.sql("show databases").show
 
//show table in a database
sqlContext.sql("show tables in default").show
 
//read the table headers
sqlContext.sql("select * from default.sample_07").printSchema

// read data from Hive
val df = sqlContext.sql("select * from default.sample_07")
//Show Table Schema 
df.printSchema

val df_renamed = df.withColumnRenamed("salary", "money") 
df_renamed.printSchema 


val newNames = Seq("code_1", "description_1", "total_emp_1", "money_1") 
val df_renamed = df.toDF(newNames: _*) 
df_renamed.printSchema 


val newNames = Seq("code_1", "description_1", "total_emp_1", "money_1") 
val df = sqlContext.sql("select * from default.sample_07").toDF(newNames: _*)



val df = sqlContext.sql("select code as code_1, description as description_1, total_emp as total_emp_1, salary as money from default.sample_07") 
 
df.printSchema

//write to Hive (in ORC format) 
df.write.format("orc").saveAsTable("default.sample_07_new_schema") 

//read back and check new_schema
sqlContext.sql("select * from default.sample_07_new_schema").printSchema

import org.apache.spark.sql.hive.orc._
import org.apache.spark.sql._


//mongo DB

val mongoDbOptions = Map(
    "host" -> "localhost:27017",
    "database" -> "mongodatabase",
    "collection" -> "mongoclient"  mongodb client
)
 
dataFrame.write
    .format("com.stratio.datasource.mongodb")
    .mode(SaveMode.Append)
    .options(mongoDbOptions)
    .save()



val mongoDbOptionsLib = Map(
    "host" -> "localhost:27017",
    "database" -> "mongodatabase",
    "collection" -> "mongoclientlib"    // mondb LIB
)    	
val libarayConfig = MongodbConfigBuilder(mongoDbOptionsLib)
dataFrame.saveToMongodb(libarayConfig.build)



//Elastic searh
val elasticOptions = Map("es.mapping.id" -> "id",
    "es.nodes" -> "localhost",
    "es.port" -> "9200",
    "es.index.auto.create" -> "yes"
)
 
dataFrame.write.format("org.elasticsearch.spark.sql")
    .mode(SaveMode.Append)
    .options(elasticOptions)
    .save(s"$elasticIndex/$elasticMapping")

// casadera

val cassandraOptions = Map("table" -> cassandraTable, "keyspace" -> cassandraKeyspace)
 
dataFrame.write
    .format("org.apache.spark.sql.cassandra")
    .mode(SaveMode.Append)
    .options(cassandraOptions)
    .save()
//Creating the physical tables and temporary external tables within the Spark SqlContext are experimental,
// if you use HiveContext only create the temporary table, for use this feature correctly you can use CrossdataContext (XDContext).

XDContext.createExternalTable("externalelastic", "org.elasticsearch.spark.sql", schema, elasticOptions)
XDContext.createExternalTable("externalmongo", "com.stratio.datasource.mongodb", schema, mongoDbOptions)
XDContext.createExternalTable("externalcassandra", "org.apache.spark.sql.cassandra", schema, cassandraOptions)
 
XDContext.sql("select * from externalelastic")
XDContext.sql("select * from externalmongo")
XDContext.sql("select * from externalcassandra")

//

//  Using HiveContext creating a link to the physical tables and storing it in Hive’s MetaStore.


hiveContext.sql(s"""CREATE TABLE IF NOT EXISTS testElastic(id STRING)
		       |USING org.elasticsearch.spark.sql
		       |OPTIONS (
		       |   path '$elasticIndex/$elasticMapping', readMetadata 'true', nodes '127.0.0.1', port '9200', cluster 'default'
		       | )
		     """.stripMargin)
 
hiveContext.sql(s"""CREATE TABLE IF NOT EXISTS testCassandra(id STRING)
		        |USING "org.apache.spark.sql.cassandra"
		        |OPTIONS (
		        |   table 'cassandraclient', keyspace 'testkeyspace'
		        | )
		     """.stripMargin)
 
hiveContext.sql(s"""CREATE TABLE IF NOT EXISTS testMongo(id STRING)
		        |USING com.stratio.datasource.mongodb"
		        |OPTIONS (
		        |  host 'localhost:27017', database 'mongodatabase', collection 'mongoclient'
		        | )
		     """.stripMargin)
 
val queryElastic = hiveContext.sql(s"SELECT id FROM testElastic limit 100")
val queryMongo = hiveContext.sql(s"SELECT id FROM testMongo limit 100")
val queryCassandra = hiveContext.sql(s"SELECT id FROM testCassandra limit 100")

import org.apache.spark.sql._
var sqlContext = new SQLContext(sc)
val sqlContext = new org.apache.spark.sql.hive.HiveContext(sc)


import org.apache.spark.sql.hive._
val hc = new HiveContext(sc)


val joinElasticCassandraMongo = hiveContext.sql(s"SELECT tc.id from testCassandra as tc" +
    	s" JOIN testElastic as te ON tc.id = te.id" +
    	s" JOIN testMongo tm on tm.id = te.id")

    val hiveContext = new HiveContext(sc)

    val kuduOptions = Map(
      "kudu.table" -> kuduAccountMartTableName,
      "kudu.master" -> kuduMaster)

    hiveContext.read.options(kuduOptions).format("org.kududb.spark.kudu").load.
      registerTempTable("account_mart_tmp")


    println("------------")
    val values = hiveContext.sql("select account_id, sum(win_count) from account_mart_tmp group by account_id").
      take(100)
    println("------------")

    values.foreach(println)
    println("------------")

    sc.stop()




// they basically contain a chain of RDDs that you can convert to DFs
mydstream.foreachRDD(rdd => {

          hiveContext.createDataFrame(rdd)
            .write
            .option("orc.compress", "snappy")
            .mode(SaveMode.Append)
            .orc("/this/is/an/hdfs/directory/too")
          })

val conf = new SparkConf().setAppName(appName) // run on cluster
val ssc = new StreamingContext(conf, Seconds(5))
val sc = ssc.sparkContext
sc.setLogLevel("ERROR")


import org.apache.spark.sql.SparkSession
val spark = SparkSession.builder().appName("Spark SQL basic example").config("spark.some.config.option", "some-value").getOrCreate()





POC failing or to be done

// If you want to have a temporary view that is shared among all sessions and keep alive until the Spark application terminates, you can create a global temporary view. Global temporary view is tied to a system preserved database global_temp, and we must use the qualified name to refer it, e.g. SELECT * FROM global_temp.view1.

train_rdd1.createGlobalTempView("blue_data")
2019-04-01 14:58:05 WARN  ObjectStore:6666 - Version information not found in metastore. hive.metastore.schema.verification is not enabled so recording the schema version 1.2.0
2019-04-01 14:58:05 WARN  ObjectStore:568 - Failed to get database default, returning NoSuchObjectException
2019-04-01 14:58:06 WARN  ObjectStore:568 - Failed to get database global_temp, returning NoSuchObjectException

spark.newSession().sql("SELECT * FROM global_temp.people").show()


// Complete AVRO in kafka / spark streaming

// spark ETL as an application?

// block index on AVRO and parque, paquer with bloom filter

KAFKA
cd /Users/keeratjohar2305/Downloads/kafka_2.11-2.1.1

// Start zoo keer services/daemon
bin/zookeeper-server-start.sh config/zookeeper.properties

// start kafka brocker/server service/daemon , you can have mutiple broker here
bin/kafka-server-start.sh config/server.properties --override delete.topic.enable=true


// Kafa topic creation and  starting kafka produces/daemon
bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic test   // create topic
bin/kafka-topics.sh --zookeeper localhost:2181 --describe  // list topic
bin/kafka-console-producer.sh --broker-list localhost:9092 --topic test   //produces on the topic  // in this broker list you specify multple broker URI which can publish message here

// kafka consumer service /dameon
bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic test --from-beginning     // start consumer


//--override delete.topic.enable=true
bin/kafka-topics.sh --zookeeper localhost:9092 --delete --topic test


publisher will publish message in the producer topic via broker machines
subscriber will listen to the message from the consumer daemon.


}
}


