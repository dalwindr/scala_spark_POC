import org.apache.spark.sql.SQLContext
import java.sql.{Connection,DriverManager}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD



//spark-shell --jars=/Users/keeratjohar2305/Downloads/mysql-connector-java-8.0.15/mysql-connector-java-8.0.15.jar
//spark-submit  --jars /Users/keeratjohar2305/Downloads/mysql-connector-java-8.0.15/mysql-connector-java-8.0.15.jar --class sql_spark --master local[*] /Users/keeratjohar2305/Desktop/practice/git_proj1/target/scala-2.11/git_proj1_2.11-1.2.7.jar

object sql_spark {

	def main(args: Array[String]) {


    val url = "jdbc:mysql://localhost:3306/testabc"
    val driver = "com.mysql.jdbc.Driver"
    val username = "testabc"
    val password = "testabc"

	//var connection:Connection = _

   try  {
        var connection = DriverManager.getConnection(url, username, password)
        val statement = connection.createStatement
        val rs = statement.executeQuery("select  id, name from example")
        while (rs.next) {
            val host = rs.getString("id")
            val user = rs.getString("name")
            println("host = %s, user = %s".format(host,user))
            }
        } catch {        
        case e: Exception => e.printStackTrace
    }

    if (args.length!=1)
    	{ println("Please input file name",args.length)
         System.exit(0)
 		}
    println("param passed:", args(0))

    //var fname= "/Users/keeratjohar2305/Downloads/59e8f3c42ef0ee849a77-ef2360e85067356b16ebd3af2689db720a47963d/inpatientCharges.csv"

    var fname=args(0)
//    var fname= "/Users/keeratjohar2305/Downloads/59e8f3c42ef0ee849a77-ef2360e85067356b16ebd3af2689db720a47963d/inpatientCharges.csv"
 val session = org.apache.spark.sql.SparkSession.builder.master("local[*]").appName("Spark CSV Reader").getOrCreate;
val df = session.read.format("com.databricks.spark.csv").option("header", "true").option("inferSchema", "true").load("file://" + fname)
df.write.format("jdbc").option("url", "jdbc:mysql://localhost:3306/testabc").option("driver","com.mysql.jdbc.Driver").option("dbtable", "testabc.wikistats_by_day_spark1").option("user", "testabc").option("password", "testabc").mode("Append").save

df.printSchema
df.registerTempTable("hospital_charges")

// all records
df.show()
df.sqlContext.sql("select * from hospital_charges").show()

// group by
df.groupBy("ProviderState").avg("AverageCoveredCharges").show
df.sqlContext.sql("select ProviderState, avg(AverageCoveredCharges) from hospital_charges group by ProviderState").show()

// group by multiple aggregate
df.sqlContext.sql("select ProviderState, avg(AverageCoveredCharges), avg(AverageTotalPayments),avg(AverageMedicarePayments) from hospital_charges group by ProviderState").show()

//group two col and aggregate one col
df.groupBy(("ProviderState"),("DRGDefinition")).sum("TotalDischarges").show() // collect.foreach(println)
df.sqlContext.sql("select ProviderState, DRGDefinition,sum(TotalDischarges) from hospital_charges group by ProviderState,DRGDefinition").collect.foreach(println)

//
df.groupBy(("ProviderState"),("DRGDefinition")).sum("TotalDischarges").sort(desc(sum("TotalDischarges").toString)).show()  // . collect.foreach(println)
df.groupBy(("ProviderState"),("DRGDefinition")).sum("TotalDischarges").orderBy(desc(sum("TotalDischarges").toString)).show()
var new_Data = df.sqlContext.sql("select ProviderState,DRGDefinition, sum(TotalDischarges) as TotalDischarges from hospital_charges group by ProviderState,DRGDefinition order by sum(TotalDischarges) desc").show()

//new_Data.write.format("parquet").mode("append").save( "/Users/keeratjohar2305/Downloads/59e8f3c42ef0ee849a77-ef2360e85067356b16ebd3af2689db720a47963d/TAB_1/df.parque")


/*

This is also working for other code scenerio
//read from SQL
val data = sqlcontext.read.format("jdbc").option("url","jdbc:mysql://localhost:3306/testabc").option("driver","com.mysql.jdbc.Driver").option("dbtable","example").option("user", "testabc").option("password", "testabc").load()
data.show()


//read from file to RDD
var fname1= "/Users/keeratjohar2305/Downloads/pagecounts-20071209-190000"
val session = org.apache.spark.sql.SparkSession.builder.master("local[*]").appName("Spark CSV Reader").getOrCreate;
val df2 = session.read.format("com.databricks.spark.csv").option("delimiter", " ").load("file://" + fname1)
//.option("header", "true").option("inferSchema", "true")

//RDD infer schema
var myschema = Seq("project_name","title","num_requests","content_size")
df2.toDF(myschema: _*).createOrReplaceTempView("test")


// agrreaget from above rdd
var df_aggr= session.sql("select count(*) as cnt, sum(num_requests) as tot_visits ,title from test group by title")

df_aggr.write.jdbc(url="jdbc:mysql://localhost:3306/testabc?user=testabc&password=testabc"), table="testabc.wikistats_by_day_spark", mode="append")

df_aggr.write.format("jdbc").option("url", "jdbc:mysql://localhost:3306/testabc").option("driver","com.mysql.jdbc.Driver").option("dbtable", "testabc.wikistats_by_day_spark").option("user", "testabc").option("password", "testabc").mode("Append").save

*/

}
}
