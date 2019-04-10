
case class Books ( title: String , pages: Int)
case class Students( name: String,age: Int, year: String)

object coll{

def main(args:Array[String]){


var books=Seq(Books("MAth",23),Books("Eng",44),Books("Phy",93))
var students=Seq(Students("Dalwinder",22,"1st year"),Students("RAvinder",34,"2nd Year"),Students("Kulwinder",35,"3rd years"))

println(books.filter(x=>x.pages>44 ))

println(students.filter(x=>x.age>24 ))
println(s"",
books.maxBy(book => book.pages),
books.minBy(book => book.pages)
)



  val abcd = Seq('a', 'b', 'c', 'd')
     val efgj = Seq('e', 'f', 'g', 'h')
     val ijkl = Seq('i', 'j', 'k', 'l')
     val mnop = Seq('m', 'n', 'o', 'p')
     val qrst = Seq('q', 'r', 's', 't')
     val uvwx = Seq('u', 'v', 'w', 'x')
     val yz = Seq('y', 'z')
     val alphabet = Seq(abcd, efgj, ijkl, mnop, qrst, uvwx, yz)
  println(alphabet)
  println(alphabet.flatten)



val a=List(1,2,3,4,5)
  val b=List(11,22,333,44,55)
  val c=List(111,222,333,444,555,666)
  val abc = List(a,b,c)
  println(abc)
  println(abc.flatten)

var mytabu = List.tabulate(5)(x=> x*10)
var mystr = "This is dalwinder singh blind"
var mylist= mystr.split(" ").toList
println(mytabu,"\n" , mylist)

println(mytabu zip mylist)
println(mylist zip mytabu)
var vec=mytabu.indices zip mylist
vec.reverse
println("This is it",vec)

vec.unzip


println("This is new things List to array")
var lifestr="This is dalwinder singh alone in the world with kids , lot his beloved jasmeet kaur"
var lifelist=lifestr.split(" ").toList
var lifeArr: Array[String] = Array.fill(lifelist.length)("x")
println(lifeArr.toList)
lifelist copyToArray lifeArr
println(lifeArr.toList)

var NewData=List(2,3,4,2,2,4534,435,2,34,324,6,4,4554,345,56,345,46,64,42,2,35,46,345,64,74,1,1,34,5,7,0,9,564,352,234,25,64)
var iterator1 = NewData.iterator
for(x<- 0 until NewData.length ) {
    print(s" "+iterator1.next)
}
println("\n")

println(NewData.sorted)

println(NewData.sortWith(_>_))

println(NewData.sortWith(_<_))

var NewData1=List("app","zip","apple","orange","mango","pineApple","grapes","blacks","tomato","ladyFinger","books","tables")

println(NewData1.sorted)

println(NewData1.sortWith(_>_))

println(NewData1.sortWith(_<_))


println(NewData1.sortWith(_.length>_.length))
println(NewData1.sortWith(_.length<_.length))


println ("\nhigher order funtion----- map function usage and flatten function usage\n")
println("----example0  map")
println(NewData.map(_+100))
println(NewData.map(_*100))
println(NewData1.map(_.length))
println(NewData1.map(_.toList))
println(NewData1.map(_.toList.reverse.mkString))
//println(NewData1.map(_.toList.flatten))


//var ssss= "This is  dalwinder singh , a looser"
//println(ssss.reverse)

println("----example1 flatten\n")
println(NewData1.map(_.reverse))

var splitted = NewData1.map(_.toList)
println(splitted.flatten)

println("----example2  map\n")
println(NewData)



println("""flat map nothing but flatten map it perform map function generate list input flatten function to spread the putput ... item 1,2,3,4->(MAP) ->List 1,2,3,4 -> (FLATTEN)-> List())""")

println("\nobserver...1 map vs FlapMap\n")
println(NewData1.map(_.toUpperCase))
println(NewData1.flatMap(_.toUpperCase))
println(NewData1.flatMap(x => List(x.toUpperCase.reverse)))

println("\nobserver...2 Usage of MAP collection\n")
println(NewData1)
println(NewData1.indices.map(x=>NewData1(x)))
var listofmap=NewData1.indices.map(x=>Map(x->NewData1(x))).toList
println("\n Dummy list of map created from String List")
println(listofmap)
//println(listofmap.map(x=> List(List(1))))
println(listofmap.map(x=> x.map(y=> y._1)))
println(listofmap.map(x=> x.map(y=> y._2)))
println(listofmap.map(x=> List(((x.map(y=> y._1)).mkString),(x.map(y=> y._2)).mkString)))

println(NewData.max)
println(NewData.min)
println(NewData.filter(444>_))
println(NewData.filter(444>_))
println(NewData.filter(333>_))
println(NewData.find(2>_).mkString.toInt)
println(NewData.find(5>_).mkString.toInt)
println(NewData.find(25>_))
println(NewData.reduce((x,y)=> x+y))
println(NewData.reduce((x,y)=> x * y))
println(NewData.reduce((x,y)=> x min y))
println(NewData.reduce((x,y)=> x  max y))
println(NewData.head)
println(NewData.tail)
println(NewData.sortWith(_>_))
println(NewData.sortWith(_<_))
println(List.fill(2)("dalwinder"))
println(List.fill(2, 3)("dalwinder"))
println(List.fill(3, 2)("poonam"))

println(( List.fill(1)("manoop"),  List.fill(1)("poonam") ).zipped.map(_ + " loves " +  _))
println((List("abc", "de"), List(3, 2)).zipped.forall(_.length == _))
println((List("abc", "de"), List(3, 2)).zipped.exists(_.length != _))

//println(( List.fill(17777)("manoop"),  List.fill(17777)("poonam") ).zipped.map(_ + " loves " +  _))


import sys.process._
"ls -l"!
val result = "ls -al" !
//println(result)

System.exit(1)

println("\nobserver... \n")
var num1 = List(1,4,7,10,13,15)

val func1 =(x: Int) => List(x-1,x,x+1)

for(x<-num1)(println(func1(x)))

println(num1.map(func1(_)))

println(num1.flatMap(func1(_)))

print("\n")

System.exit(1)


var numstrxx = "11 2 5 4 5 16 7 8 9 10 11  "
var numstrarr = numstrxx.split(" ")
var numstrlist=numstrarr.toList
println(numstrlist.map(_.toInt))
print("\n")

var numstrList = List(numstrxx)
println(numstrList)
var kss = numstrList.flatMap(_.toString).map(_.toString).filter(_!=" ").map(_.toInt) //.sorted
println(kss)


def MyInt(x: String ): Option[Int] ={
try {
     Some(Integer.parseInt(x.trim))
    }
catch
{
    case e: Exception => None
}
}





var numstrxx1 = "11 2 5 4 5 16 7 8 9 10 11 sss  sss sss "
var numstrarr1 = numstrxx1.split(" ")
var numstrlist1=numstrarr1.toList
//
println("Vao",numstrlist1.flatMap(MyInt))
//numstrlist1.foreach(println)

//var noneD: List[Int]=List(None,None,None,2)
//println(noneD.flatMap(_.toInt))


}
}

