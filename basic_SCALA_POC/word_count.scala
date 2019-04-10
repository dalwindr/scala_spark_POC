import scala.io.Source
//var filename = "/Users/keeratjohar2305/Downloads/test_lp.csv"
var input = Source.fromFile("/Users/keeratjohar2305/Downloads/test_lp.csv")
var fdata = input.getLines.toList
var counts = new collection.mutable.HashMap[String,Int].withDefaultValue(0)
fdata.map{word => val x = word.split(","); x.map( cnt => counts(cnt) += 1)}
println("Tolal words  in ",counts)

var pdata=List("this is dalwinder singh","this is dalwinder singh alone in the useless world")
var counts1 = new collection.mutable.HashMap[String,Int].withDefaultValue(0)
pdata.map{word => val x = word.split(" "); x.map( cnt => counts1(cnt) += 1)}
println("Tolal words  in ",counts1)
