import scala.xml._
import scala.collection.mutable.HashMap
import scala.collection.immutable.{TreeMap, TreeSet}
import scala.collection.mutable.ListBuffer
import scala.collection.mutable.ArrayBuffer

class Point(val xc: Int, val yc: Int) {
var x: Int = xc
var y: Int = yc
var k = 10
def move(dx: Int, dy: Int) {
x = x + dx
y = y + dy
k =20
println ("Point x location : " + x);
println ("Point y location : " + y);
}
}

class Location(override val xc: Int, override val yc : Int, val zc : Int ) extends Point( xc, yc) {
var z: Int = zc

def move (dx: Int, dy: Int, dz:Int) {
   x+=dx
   y+=dy
   z+=dz
    println ("Point x location : " + x);
    println ("Point y location : " + y);
    println ("Point z location : " + y);
    }
}


object newFile {

def main(args: Array[String])
{
//val s = "Hello"; println(s)

            def linespace() = println(s"")

//var lsp: Unit = println("\n")

val dataypes_in_scala = "Bype,Short,Int,Long,Float,Double,Char,String,Boolean,Unit,Null,Nothing,Any,AnyRef";
println ("Scala Support Data Types:      \n     "  + dataypes_in_scala )

var mystr: String ="My String"
var myint: Int = 2332
var mydouble: Double = 2332.123
//var mynothing: Float = 213.1233
var mychar: Char = 'D'
var myshort: Short = 222
var mybyte: Byte = 111
var mybool: Boolean =true
val (myVar1, myVar2) = Pair(40, "Foo")
println (mystr,myint,mydouble,mychar,myshort,mybyte,mybool,myVar1,myVar2)

//var mylist: List(1321,33)

println("first object")
var pObj1 = new Point(10,19)
pObj1.move(10, 10)
pObj1.move(20, 20)


println("second object")
var pObj2 = new Point(12,17)
pObj2.move(10, 10)

println("Third object")

var lObj1 = new Location(10,19,8)
lObj1.move(10, 10,10)


println (pObj1.x,pObj1.y,pObj1.k)
println("lesson 3: if statements.....................")
linespace
//4 times println("Hellow")

def division(current_marks: Int) : Unit = {
    if (current_marks>75 ) println("Distinction")
    else if(current_marks>60 ) println("2nd division")
    else  if(current_marks>33 ) println("3rd division")
    else println("Fail")
   }

var dd=division(36)



println("lesson 4: loops.....................")



linespace
var wl=0
while(wl<=5) {println("While loop",wl);wl=wl+1}
do{println("do While loop",wl);wl=wl+1} while (wl<10)
for (wl <- wl to 12) {println("for loop",wl)}
for (wl <- wl until 22){println("for loop",wl)}

var listd=List(1,2,3,4,5)
listd.foreach(println)

var liste :List[String] = List("App","MAN","Sun","Mon","Tue")
liste.foreach(println)
var datanew= List(33,44,55,66,777,888)

for (x <- datanew if x >=33 ; if x< 66) println (x)

println("You can create a for loop with two counters like this:")
for (i <- 1 to 2; j <- 1 to 2) println(s"i = $i, j = $j")

//System.exit(1)




println("lesson 5: funcions................")





linespace
def addNum(x: Int, y: Int): Int =  x + y
def sqrNum(x: Int): Int = x * x
def sumSqr(x:Int , y: Int ) : Int =sqrNum(x) + sqrNum(y)
def linetab() = println("hello")
//linetab
println("add Num=",addNum(1,3),"sql num=",sqrNum(20), "sum of sqrs=",sumSqr(5,7))



def IntTimes(x: Int,f:() => Unit ): Unit= {
                   if (x>0) {
                        f()
                        IntTimes(x-1,f)
                    }

        }
IntTimes(3, linetab)



println("lesson 6: Closure................")
linespace
val factor=3
val Multiplier= (i: Int) => i * factor



println("first call to Multiplier(closure function)" , Multiplier(7))

println("second call to Multiplier(closure function)" , Multiplier(8))




println("lesson 7: String and interpolator ................")


linespace
var name: String = "Johar"
var subject: String ="Hinde"
var marks =  20.3244
var totalmark = 30
var mystring1: String = "This is my scala program"
var mystring3: String = "      This is my scala programs        "
var mystring4: String = "This is my scalar programs"
var mystring2: String = " and I am hadoop learning"
println(mystring1+ mystring2,mystring2.length())

printf("My son %s  got %s out of %s marks in %s with printf \n",name,marks+3,totalmark,subject);

println(s"My son $name  got ${marks + 3} out of $totalmark  marks in $subject  With s interpolator \n");
println(raw"My son $name  got ${marks + 3} out of $totalmark  marks in $subject With raw interpolator \n");
println(f"My son $name%s  got ${marks + 3} out of $totalmark  marks in $subject With f interpolator  \n");

println(mystring1,mystring1.charAt(0),mystring3.toUpperCase(),mystring3.trim())
var strTolist1=List(mystring1)
//for
linespace

//var dd_array=mystring3.toCharArray()
//println(dd_array)


//System.exit(1)
linespace



println("lesson 7: Array  and creation................")




var mystrarr: Array[String] = new Array[String](3)
var mynumarr: Array[Int] = new Array[Int](5)

mystrarr(0)="Dawinder singh,"
mystrarr(1)="Always recollecting wife memories"
mystrarr(4/2)=".He, Just passing time in this chutia world without his beloved wife (jasmeet kaur-poonam)"

println(mystrarr.mkString(" "))
linespace
var mystrarrList: List[String]=List()


println("...Split array into string to list.........")

//Split array into string
val newstringxyz: String =mystrarr.mkString(" ")
//newstringxyz.split(' ').foreach(println)
newstringxyz.split(' ').foreach(mystrarrList::=_)


for (x<-mystrarrList)println("Hi",x)

//newstringxyzs.split(' ')
//.foreach(mystrarrList::=_)
//linespace

//mystrarrList.foreach(println)

//linespace
//println(mystrarrList)

//mynumarr=tabulate(5)(x => x*x))

//linespace
//System.exit(1)



println("lesson 8: collection (lists,maps,sets,options,tuples,Iterators)................")




var mylist1: List[String] = List()
mylist1::="dalwinder"
mylist1=mylist1:::"Singh"::Nil
mylist1::="Hi"
mylist1=mylist1:::"SSA Ji"::Nil
print(mylist1)

var mylistnum : List[Int] = List()
mylistnum::=20
mylistnum=mylistnum:::40::Nil
mylistnum::=10
mylistnum=mylistnum:::50::Nil
println(mylistnum)

//var mylistnum1 : List[Int] = List.tabulate(5)(x => x)
//var mylistnum2 : List[Int] = List()
println(List.tabulate(5)(x => x)+s"tabulate")
println(List.tabulate(5)(x => x+1)+s"tabulate")
println(List.tabulate(5)(x => x+2)+s"tabulate")
println(List.tabulate(5)(x => x+3)+s"tabulate")
println(List.tabulate(5)(x => x+4)+s"tabulate")
println(List.tabulate(5)(x => x+x)+s"tabulate")
println(List.tabulate(5)(x => x*x)+s"tabulate")
linespace
println(List.range(5,10)+s"range")
linespace
println(List.iterate(1,5)(x=> x)+s"iterate")
println(List.iterate(1,5)(x=> x+1)+s"iterate")
println(List.iterate(1,5)(x=> x+2)+s"iterate")
println(List.iterate(1,5)(x=> x+3)+s"iterate")
println(List.iterate(1,5)(x=> x+4)+s"iterate")
linespace
println(List.iterate(1,5)(x=> x+x )+s"iterate")
println(List.iterate(1,5)(x=> x+x+x )+s"iterate")


//String to list
var mystr11: String = "This is dalwinder singh , he lives in gunjur"
var mynewList : List[String] = List()
mystr11.split(' ').foreach(mynewList::=_)
print(mynewList)

mylistnum.foreach(println)

mylist1.foreach(println)

linespace


println ("...list yield with for loop...")
val data = List(1, 2, 3, 4, 5,6,7,8,9,10,11,12,13,14,15,16)


var res=for{ x <- data
            if x> 2; if x< 8 }
            yield x

for (x <- res
        if x >2 ; if x< 15)
      println (x)


var myseq1=Seq(12,1221,3344,5455)
var myseq2=Seq("SUN","MON","TUE","WED","THU","FRI","SAT","sdfs  asdf a")
println(myseq1)
println(myseq2)
println(s"${myseq2(3)}")
println(s"${myseq1(3)}")

myseq1=myseq1++Seq(1,2,3,4,5)

var myseq3=myseq1:+234
var myseq4=myseq3++Seq(12321,123123,242,24,234,234)
myseq1:+237
myseq1:+23
println(myseq1,myseq3,myseq4)


var Theory = """
tranversal  -> iterable
            1) Seq
                   a)Linear seq ( List,Stack,Stream,Queue)
                   b)Indexed seq (range,NumericRange,String,Vector)

            2) Map ( Hash,List,Sorted{Bit,Tree})


            3) Set  ( Hash,List,Sorted{Bit,Tree})

       """

println(Theory)
var emptySeq: Seq[String] = Seq()
emptySeq=emptySeq:+"TEST"
println(emptySeq)


println("Complex example for Binary to Decimal and Decimal to binary..................")


def bin2Dec(x: List[Int] ): Int =
{

var powerNum = List.tabulate(x.length)(x=>(math.pow(2,x)toInt))

var sum=0
for ( i <- 0 to x.length -1 )
{sum+=powerNum(i)*x(i)}
return sum

}


println(bin2Dec(List(1,0,1,1)))
println(bin2Dec(List(1,1,1)))

//Dec2Bin

def Dec2Bin_calc(dec: Int): List[Int]={

var list: List[Int] = List()
var Num:  List[Int] = List()
var quot: Int = dec/2
var rem = dec%2

if (dec == 1 )
    return List(1)

//println(quot,rem)
if (quot > 1 )
{
list = list:::dec%2::Nil

//println("calling recursion Number",Num,quot)

Num=Dec2Bin_calc(quot)

list = Num:::list
//println(s"new list="+list+" with "+ rem)
}
else
{
//println("termination recursion Number",quot,rem)

list = list:::quot::rem::Nil
}

//println("Number end="+dec/2+" and List is="+dec%2)
return list
}

for (i<- 1 to 20) {println(s""+i+"="+Dec2Bin_calc(i))}

var bin = ""
var   num= 0
for (i<- 1 to 20)
{
 bin=i.toBinaryString
 num=Integer.parseInt(bin,2)
 println(s""+num+"="+bin)

}


var test = List.tabulate(10)(x=>x)

println(test.map(x=>if (x%2==0) (math.pow(2,x)).toInt else (math.pow(2,x)).toInt*(-1)))

//map(lambda x: 2*x if x % 2 == 0 else x/2, l)


}
}
