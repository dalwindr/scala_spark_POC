

import collection.mutable.HashMap
import collection.mutable.HashSet
/*
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD
import org.apache.spark.rdd.RDD._
import org.apache.spark.rdd.PairRDDFunctions
import scala.collection.Map
import org.apache.spark.SparkContext.rddToPairRDDFunctions
*/
class Search(Str: String) {
    def isMatch(se:String):Boolean = {
        Str.contains(se)
    }
}

case class Score(name: String,  subject: String,   marks: Int)


object add{
def main(args: Array[String]){

//val conf = new  SparkConf.setAppName("test")
//val sc =  new SparkContext(conf)

var HMap_d : HashMap[String,Int] = HashMap(("Dalwinder",1),("poonam",2),("Dalwinder",1))

var HSet_d : HashSet[String] = HashSet("Dalwinder","Dalwinder","singh","pooname","pooname")

println(HMap_d.getClass, HSet_d.getClass)

println(HMap_d)
println(HSet_d)

var Map_d1 : HashMap[String,Int] = HashMap(("poonam",1))
var Map_d2 : HashMap[String,Int] = HashMap(("dalwinder",1))
var Map_d3 : HashMap[String,Int] = HashMap(("dalwinder",1))

var HMap_collectionbase = new collection.mutable.HashMap[String,Int]
var HMap_normal = Map_d1 ++ Map_d2 ++ Map_d3   //Hold duplication records

//operation not allow HMap_collectionbase = Map_d1 ++ Map_d2 ++ Map_d3

HMap_collectionbase = Map_d1 ++ Map_d2 //++ Map_d3

HMap_collectionbase = HMap_collectionbase ++ Map_d3   // hold unique records

println("\n")

println("HMap_collectionbase:",HMap_collectionbase)
println("HMap_d : ............ \n",HMap_normal)

println("\n")

def addtoHP = (myHP: collection.mutable.HashMap[String,Int], mykey: String , myval: Int ) =>  myHP(mykey)=myval

addtoHP(HMap_d,"ram",1)
println("Data added to HMap_d .............\n",HMap_d)

println("\n")


val addToSet= (myset:collection.mutable.HashSet[String], str:String) => myset += str


println("dalwinder added to set \n ", HSet_d)
println("\n")

addToSet(HSet_d,"x")
addToSet(HSet_d,"y")
addToSet(HSet_d,"y")
addToSet(HSet_d,"x")
addToSet(HSet_d,"y")
addToSet(HSet_d,"y")

val N = Set("sddd") ++ Set("ddddd") ++ Set("sdfsfs ")

//case  newS (x: String) =>  var HSet_d : HashSet[String] = HashSet(x); HSet_d


println("dalwinder added to set \n ", HSet_d)

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

var input= List( "good line1","good line2","warn line2","warn line2","error line2","error line2")
//var lines=sc.parallelise(input)
var goodline= input.filter(x=>x.contains("good"))
var errorline= input.filter(x=>x.contains("error"))
var warnline= input.filter(x=>x.contains("warn"))
println("\n good data is :  ",goodline, "\n error data is: ",
errorline, "\n warn data is :  ",
warnline,"\n")
}



ds for partition 0

emp_data.mapPartitionsWithIndex( (index: Int, it: Iterator[String]) =>it.toList.map(x => if (index ==0) {println(x)}).iterator).collect()


}
