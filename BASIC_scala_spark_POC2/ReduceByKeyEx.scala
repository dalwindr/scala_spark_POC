
package rdd.count

import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.{SparkConf, SparkContext}

// Data use Case
// process data  only with scala without spark
// process data with spark  Regular keyed RDD (val1,val2)      -- map function  will work but not mapValues 
// process data with spark   (key, val)  RDD        -- map function work  , but mapValues also  --- but observe the difference
// process data with spark   (key, (val1,val2)  paid RDD - multiple values    -- map function work  , but mapValues also  --- but observe the difference
// process data with spark   (key1,key2), (val1,val2)  paid RDD - Mupltiple key ,multiple values RDD -- map function work  , but mapValues also  --- but observe the difference


object  ReduceByKeyEx{

  def main(args: Array[String]) {
    Logger.getLogger("org").setLevel(Level.ERROR)
    val conf = new SparkConf().setAppName("count")
    val sc = new SparkContext(conf)

    println("Welcome to scala world")
    val inputWords = List("spark", "hadoop", "spark", "hive", "pig", "cassandra", "hadoop")
    val numWords = List(1,2,3,4,5,324,5,324,435,746,546,324,435,746,546,324,435,746,546,57,435,456,57,24,13,234,46,46,46,46,46,46,46,46,46,46,6,65,53,4)

  
    //Non -RDD  operations      -- start   Lesson 1
    
    numWords.aggregate(0)((x,y)=> math.min(x, y),(p,q)=> p + q) //Min  using aggregate
    numWords.aggregate(0)((x,y)=> math.max(x, y),(p,q)=> p + q) //Max  using aggregate
    numWords.aggregate(0)((x,y)=> (x + y),(p,q)=> p + q)   //sum  using aggregate

    numWords.reduce((x,y)=> (x+y)) //  //sum  using aggregate
    numWords.reduce((x,y)=> Math.max(x,y))  //Min  using aggregate
    numWords.reduce((x,y)=> Math.min(x,y))   //Min  using aggregate

    println(numWords.length,inputWords.length)   // count

    //Non -RDD  operations   --- over

    val wordRdd = sc.parallelize(inputWords)
    val numRdd = sc.parallelize(numWords)

    println("wordRdd Count: " + wordRdd.count())
    println("numRdd Count: " + numRdd.count()) 


    //RDD  operations      -- start   Lesson 2
    
    numRdd.aggregate(0)((x,y)=> math.min(x, y),(p,q)=> p + q) //Min  using aggregate
    numRdd.aggregate(0)((x,y)=> math.max(x, y),(p,q)=> p + q) //Max  using aggregate
    numRdd.aggregate(0)((x,y)=> (x + y),(p,q)=> p + q)   //sum  using aggregate

    numRdd.reduce((x,y)=> (x+y)) //  //sum  using aggregate
    numRdd.reduce((x,y)=> Math.max(x,y))  //Min  using aggregate
    numRdd.reduce((x,y)=> Math.min(x,y))   //Min  using aggregate


    //RDD  operations   --- over Lesson 2

  
    //RDD Key values by reduceByKey Lesson 3

    println("reduce() on RDD is  used where we want to sum/combine/add all the element = " + numRdd.reduce((x,y) => x+y ))
    println("reduce() on RDD is  used where we want to sum/combine/add all the element = " + wordRdd.reduce((x,y) => x+ " " + y ))


    println(" Observer \n ")
    println(" We have following Keys operation in paid RDD")
    /*
       1) countByKey / CollectAsMap()
       2) reduceByKey
       3) aggregateByKey
       4) combinerByKey
       5) joins (LeftOuterJoin/RightOuterJoin/Join/coogroup,subtractByKey)
       6) sortBykey
       7) groupByKey
       8) Keys
       9) mapValues
       10)Values 
     
    */

    var numRdd_traformed= numRdd.map(x=>(x,1)).reduceByKey((x,y) => x+y)    //  conversion to PAIRED RDD (K,V)
    var wordRdd_traformed= wordRdd.map(x=>(x,1)).reduceByKey((x,y) => x+y)  //  conversion to PAIRED RDD (K,V)
    println("reduceByKey() on RDD is  used where we want to find occurance of one/more elements in a given list = \n"+ numRdd_traformed.collect().toList) 
    
    println("reduceByKey() on RDD is  used where we want to find occurance of one/more elements in a given list = \n"+wordRdd_traformed.collect().toList)

    


    println("\nRemember: \nreduceByKey() will aggregate key before shuffling in each split, and groupByKey will shuffle all the value key pairs with out aggregate ,So groupByKey is costly")
    println("""  reduce() is the subset of reduceByKey \n  reduceByKey is the subset of aggregateByKey like \n 1) reduce will sum up List(1,2,3,4,5) = 15\n
           reduceByKey = List((a,3),(a,6),(b,5)) = List((a,9)(b,5))
           aggregateByKey= List((a,3),(a,6),(b,5)) = List((a,set(3,6)(b,5))
 
 1. Example 

val keysWithValuesList = Array("foo=A", "foo=A", "foo=A", "foo=A", "foo=B", "bar=C", "bar=D", "bar=D")
val data = sc.parallelize(keysWithValuesList)
//Create key value pairs
val kv = data.map(_.split("=")).map(v => (v(0), v(1))).cache()

val initialCount = 0;
val addToCounts = (n: Int, v: String) => n + 1
val sumPartitionCounts = (p1: Int, p2: Int) => p1 + p2
val countByKey = kv.aggregateByKey(initialCount)(addToCounts, sumPartitionCounts)

1. Example 


val initialSet = mutable.HashSet.empty[String]
val addToSet = (s: mutable.HashSet[String], v: String) => s += v
val mergePartitionSets = (p1: mutable.HashSet[String], p2: mutable.HashSet[String]) => p1 ++= p2
val uniqueByKey = kv.aggregateByKey(initialSet)(addToSet, mergePartitionSets)

Example 3 

val result = input.aggregate((0, 0))(
(acc, value) => (acc._1 + value, acc._2 + 1),
(acc1, acc2) => (acc1._1 + acc2._1, acc1._2 + acc2._2)) val avg = result._1 / result._2.toDouble


    //println("reduce will sum up List(1,2,3,4,5)")

    //wordRdd.map(x=>(x,1)).aggregateByKey(0)((k,v) => v.toInt+k,(v,k)=>k+v).collect 

    //wordRdd.map(x=>(x,1)).aggregateByKey(new HashSet[Int])(_+_, _++_).collect()

 
         """)


    case class Score(name: String, subject:String, score:Int)

    var scores=List(
                        Score("A","Hinde",40),
                        Score("A","English",59),
                        Score("A","Punjabi",60),
                        Score("B","Hinde",49),
                        Score("B","English",78),
                        Score("B","Punjabi",93),
                        Score("E","Hinde",27),
                        Score("E","English",49),
                        Score("E","Punjabi",49)
                    )

    scores.foreach(x=> println(x.score))

   var input= List( "good line1","good line2","warn line2","warn line2","error line2","error line2")
   var lines = sc.parallelize(input)    
   
   def isMatch(str: String, se:String) :Boolean = { 
         str.contains(se)
   }

   def rddSearch(rdd: RDD[String],se: String) : RDD[String] = { 
        rdd.filter(x=>isMatch(x,se))
    }


    var goodline=rddSearch(lines,"good") ; var errorline=rddSearch(lines,"good") ; var warnline=rddSearch(lines,"good") ; 
    
     goodline.collect()
     errorline.collect()
     warnline.collect()

    var result = sc.parallelize(List(1,2,3,4,5,6,3,4,3,2,555,64,3,3344))
    var xout = result.collect().mkString(" x  ")
    println("xout: ",xout)
    var r1 = sc.parallelize(List.range(1,5))

    var nar = Array(Array(("k1",Array("v1","v11")),("k2",Array("v2","v33"))), Array(("k3",Array("v3","v33")),("k4",Array("v4","v44"))))   


    // RDD support intersection , distinct, union, subtract, cartesian

    result.distinct().collect
    r1.union(result).collect
    r1.intersection(result).collect
    r1.union(result).distinct.collect
    r1.subtract(result).collect
    result.subtract(r1).collect
    val un=result.cartesian(r1)
    

    //aggregate by key is super set of reduceByKey
    // aggregate can perform the task of reduceByKey but reverse is not possible
    //Also expercise
    //http://www.itversity.com/topic/aggregations-reducebykey-and-aggregatebykey/
    
    // 1. initializing collection.mutable.HashSet(0)
    // 2. merging and adding with starting initial number  (x,y)=> x += y  //Local Combiner/merging with acculamulating 
    // 3. Combiner (p1,p2)=> p1 ++ p2)      // combining output from different nodes

    //rddById.aggregateByKey(collection.mutable.HashSet(0,0))((s: mutable.HashSet[(Int, Int)], v: (Int, Int)) => s += v , (p1: mutable.HashSet[(Int, Int)], p2: mutable.HashSet[(Int, Int)]) => p1 ++= p2 )

    //Example 1
    un.aggregateByKey(collection.mutable.HashSet(0))( (x,y)=> x += y, (p1,p2)=> p1 ++ p2).collect()
  
    //Example 2
    un.aggregateByKey(0)((x,y)=> x + 1, (p1,p2)=> p1 + p2).collect()
  
    //Example 1   
    un.aggregateByKey(List[Int]())( (x,y) =>  x++List(y) , (p1,p2) => p1 ++p2).collect()

    un.aggregateByKey(0)((x,y)=> x + 1, (p1,p2)=> p2*p1).collect()    // observe ...  the value generated in middle (1) function is multiplying 
  

    // group by key is costly then reduceByKey
    //reduceByKey used mainly for (min/max/sum)
    un.reduceByKey((x,y)=> x+y).collect()
    un.reduceByKey((x,y)=> Math.max(x,y)).collect() //
    un.reduceByKey((x,y)=> Math.min(x,y)).collect() //

    // group by key ( min/max/sum-> all) but not recommmended - shuffling & network traffice
    un.groupByKey().map(x =>(x._1, x._2.sum)).collect()
    

    // countByKey is special case ( subset) of reduceByKey
    // reduceByKey perform the task of countByKey and reverse is not possible
    un.countByKey()
    un.collectAsMap()
    un.lookup(2)
    un.lookup(1)


    //CombineByKey   combineBy Key(createCombiner, mergeValue, mergeCombiners, partitioner)
    //Combiner function is used in the aggregate by /reduce by  function
    //1. (x:Double) => (1,x)  -- //Output type is (int,double)  
    //2. (Data: (Int,Double),Tot: Double) => ( Data._1 + 1, Tot + Data._2)  //Local Combiner/merging with acculamulating 
    //3. (Num1:(Int,Double),Num2:(Int,Double))=> ( Num1._1 + Num2._1,Num1._2 + Num2._2)   // combining output from different nodes
   
    // Exp 1   
    val score = Array(("Ajay", 98.0), ("Bill", 85.0), ("Bill", 81.0), ("Ajay", 90.0), ("Ajay", 85.0), ("Bill", 88.0))
    val rdd = sc.parallelize(score)
    var rdd1= rdd.combineByKey( (x:Double) => (1,x), (Data: (Int,Double),Tot: Double) => ( Data._1 + 1, Tot + Data._2) , (Num1:(Int,Double),Num2:(Int,Double))=> ( Num1._1 + Num2._1,Num1._2 + Num2._2) ).collect
    //avg calculation is a saperate step
    rdd1.map{ case(x,y) => (x , y._2 / y._1 ) }.collect()

    //Exp 2
    //1. initializing //(x:Int)=>(1,x)
    //2. Local Combiner //(v:(Int,Int),tot:Int)=>(v._1+1,v._2+tot)  with acculamulator
    //3. partiton Combiner //(n1:(Int,Int),n2:(Int,Int))=>(n1._1+n2._1,n1._2+n2._2))
    union_tot=un.combineByKey((x:Int)=>(1,x), (v:(Int,Int),tot:Int)=>(v._1+1,v._2+tot),(n1:(Int,Int),n2:(Int,Int))=>(n1._1+n2._1,n1._2+n2._2)).collect()
    //avg calculation is a saperate step
    fdatax.map(x=>x.split(",").toList).map{ x => ( x(1), x(2).toFloat)}.combineByKey((x:Int) => (1,x), (num:(int,Int),tot) => (num._1 + 1, num._2 + tot), (n1,n2) => (n1._1 + n2._2, n1._2 + n1._2))
    
    union_tot.map{case (x,avgsrc) => (x, avgsrc._2/avgsrc._1)}.collect

    println( "\n Note:\n Subtract,intersection,distinct All these are expensive operation and requires shuffling but not with union case")

    
    println("cogroup, join, leftOuterJoin, rightOuterJoin, crossjoin ,fullOuterJoin")
    val rdd1 = sc.parallelize(Seq(
                     ("key1", 1),
                     ("key2", 2),
                     ("key1", 3),
                     ("key1", 3),
                     ("key3", 3),
                     ("key4", 3),
                     ("key4", 3)
                    ))
    val rdd2 = sc.parallelize(Seq(
                     ("key1", 5),
                     ("key2", 4),
                     ("key5", 4),
                     ("key6", 4)

                        ))

    rdd1.join(rdd2).collect
    //Array((key1,(1,5)), (key1,(3,5)), (key1,(3,5)), (key2,(2,4)))

    rdd1.leftOuterJoin(rdd2).collect
    //Array((key3,(3,None)), (key4,(3,None)), (key4,(3,None)), (key1,(1,Some(5))), (key1,(3,Some(5))), (key1,(3,Some(5))), (key2,(2,Some(4))))
    
    rdd1.rightOuterJoin(rdd2).collect
    //Array((key5,(None,4)), (key1,(Some(1),5)), (key1,(Some(3),5)), (key1,(Some(3),5)), (key2,(Some(2),4)), (key6,(None,4)))
    
    rdd1.fullOuterJoin(rdd2).collect
    //Array((key3,(Some(3),None)), (key4,(Some(3),None)), (key4,(Some(3),None)), (key5,(None,Some(4))), (key1,(Some(1),Some(5))), 
    //(key1,(Some(3),Some(5))), (key1,(Some(3),Some(5))), (key2,(Some(2),Some(4))), (key6,(None,Some(4))))


    rdd1.cogroup(rdd2).collect
    //Array((key3,(CompactBuffer(3),CompactBuffer())), (key4,(CompactBuffer(3, 3),CompactBuffer())), 
    //(key5,(CompactBuffer(),CompactBuffer(4))), (key1,(CompactBuffer(1, 3, 3),CompactBuffer(5))), 
    //(key2,(CompactBuffer(2),CompactBuffer(4))), (key6,(CompactBuffer(),CompactBuffer(4))))

    rdd1.cartesian(rdd2).collect
    //Array(((key1,1),(key1,5)), ((key1,1),(key2,4)), ((key1,1),(key5,4)), ((key1,1),(key6,4)), ((key2,2),(key1,5)), ((key1,3),(key1,5)), 
    //((key2,2),(key2,4)), ((key1,3),(key2,4)), ((key2,2),(key5,4)), ((key1,3),(key5,4)), ((key2,2),(key6,4)), ((key1,3),(key6,4)), 
    //((key1,3),(key1,5)), ((key3,3),(key1,5)), ((key1,3),(key2,4)), ((key3,3),(key2,4)), ((key1,3),(key5,4)), ((key3,3),(key5,4)), ((key1,3),(key6,4)), ((key3,3),(key6,4)), ((key4,3),(key1,5)), ((key4,3),(key1,5)), ((key4,3),(key2,4)), ((key4,3),(key2,4)), ((key4,3),(key5,4)), ((key4,3),(key5,4)), ((key4,3),(key6,4)), ((key4,3),(key6,4)))

    rdd1.subtractByKey(rdd2).collect
    //Array((key3,3), (key4,3), (key4,3))

    rdd2.subtractByKey(rdd1).collect
    //Array((key5,4), (key6,4))


//Advance Tutorial Lesson 4
// reduceByKey ( multiple Key) - single values
var myrdd = sc.parallelize(List(("A","M","34"), ("B","M","34"),("A","M","34")))
myrdd.collect()

myrdd.map(x=> ((x._1,x._2),x._3)).reduceByKey((v1,v2)=> (v1.toInt + v2.toInt ).toString).collect() 
myrdd.map(x=> ((x._1,x._2),x._3)).reduceByKey((v1,v2)=> (v1 + v2 )).collect()

// reduceByKey ( multiple Key) - multiple values
var myrdd_mv = sc.parallelize(List(("A","M","34","46"), ("B","M","34","78"),("A","M","34","67")))
myrdd_mv.collect()

myrdd_mv.map(x=> ((x._1, x._2),(x._3, x._4))).reduceByKey{ case((x1, x2), (y1, y2)) => ( x1 + y1, x2 + y2 )}.collect

//import scala.io.Source
// var fname = Source.fromFile("real_state.csv")
// var fheader= fname.head
// var fdata= fname.drop(1).toList
// var fdataRDD = sc.parallelize(fdata)
//fdataRDD.map(x=>x.split(",").toList).map{ x => ( x(1), (x(2).toFloat, x(3).toFloat))}.reduceByKey((x,y)=>(x._1+y._1,x._2+y._2)).map(x => (x._1,(x._2._1/x._2._2))).collect
//fdatax.map(x=>x.split(",").toList).map{ x => ( x(1), (x(2).toDouble, x(3).toDouble))}.reduceByKey((x,y)=>(x._1+y._1,x._2+y._2)).map(x => (x._1,(x._2._1/x._2._2))).collect
//fdatax.map(x=>x.split(",").toList).map{ x => ( x(1), x(2).toDouble)}.combineByKey((x:Double)=> (1.0,x), (num: (Double,Double),tot: Double ) => (num._1 + 1, num._2 + tot), (n1:(Double,Double) , n2:(Double,Double)) => ( n1._1 + n2._2, n1._2 + n1._2)).collect()


val inputrdd = sc.parallelize(Seq(("maths", 50), ("maths", 60), ("english", 65)))
inputrdd.reduceByKey((x, y) => (x+y)).collect()

val mapped = inputrdd.mapValues(mark => (mark, 1));
//Array((maths,(50,1)), (maths,(60,1)), (english,(65,1)))

mapped.reduceByKey((x,y)=>(x._1 + y._1, x._2+y._2)).collect()
//Array((english,(65,1)), (maths,(110,2)))


//sortBykey
val inputrdd1 = sc.parallelize(Seq(2242234,24,24,342,345,435,45,42,34543,6,345,436,422,35634,34,345,36,5,33,2,234,32,2,3,33,34,23432,423,2,2,334,422,2,22,2,35,444,4,4,4,4,45,5,66,6,66,6,777,7))

inputrdd1.map(x=> (x,1)).reduceByKey((x,y)=> (x+y)).sortBy(x=>x._1,ascending=true).collect()
inputrdd1.map(x=> (x,1)).reduceByKey((x,y)=> (x+y)).sortBy(x=>x._1,ascending=false).collect()
//Array((2,6), (3,1), (4,4), (5,2), (6,3), (7,1), (22,1), (24,2), (32,1), (33,2), (34,2), (35,1), (36,1), (42,1), (45,2), (66,2), (234,1), (334,1), (342,1), (345,3), (422,2), (423,1), (435,1), (436,1), (444,1), (777,1), (23432,1), (34543,1), (35634,1), (2242234,1))

inputrdd1.map(x=> (x,1)).reduceByKey((x,y)=> (x+y)).sortBykey


//tree travesal  ->  
//  1) Depth first ( Stack)
//       a) InOrder,PreOrder,PostOrder 
//            InOrder (traverse Left,visit Node,traverse right)   
//  2) Breadth first  ( Queue) 
//         b) Level order   


var output = inputrdd1.aggregate(0,0)( (num,tot) => (num._1 + tot, num._2 + 1), (v1 ,v2) => (v1._1 + v2._1, v1._2 + v2._2))

println (output._1.toFloat/output._2)


/*
   Note : hive partitioning 
        Data is partitioned on the basis of the State/ DAte where coloumn is low cardinal column.
        clause is partition by ( "state","city")

        hive bucking do hashing on the column and should be done on the column where high cardinality.
        cluase used is clustered by 

        CREATE TABLE paccount 
        (name string,phone int,city string) PARTITIONED BY (country string) 
            CLUSTERED BY (phone) INTO 128 BUCKETS stored as ORC;
 
*/


} 
}
