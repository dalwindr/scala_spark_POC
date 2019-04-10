object my_externla{
def main(args: Array[String] )
{
println( "\nDalwinder \n")
}


var Mylist=List.tabulate(10)(x=>x*x)
print(Mylist)

var Mylist2=List.tabulate(3)(x=>x*x*x*x)
print(Mylist2)

//var l3= concat(Mylist , Mylist2)
//print(l3)

var x=1010
Mylist2= x::222::Mylist2

print("\n",Mylist2)
Mylist2=List.concat(Mylist2,Mylist)

print("\n",Mylist2)


var nls= List.range(1,6)
print("\n",nls)


var MyList=List(9,8,7)
print("\n",MyList)

var NewList=nls:::MyList
var dd=27
NewList=NewList:+dd



print("\n",NewList)

var MyNumber=25

NewList=NewList:::MyNumber::Nil

//NewList.sort()


for (x <- NewList)(println(x*2))

println(NewList)

var  d = 0
//x.foreach { println }
//println(d)




//var mul = x.map(x => x*x)
//var evens= x.filter( a => a %2 ==0)
//print(evens)
var xxx = List(1,2,3,4,5)
//var  y=x.map(x=> x*2)

//print y


var myl:List[Nothing] = List()


var xyzList="XX" ::"yy" :: "xx" :: Nil

var xyznumList=1::3::4::7::8::Nil
println(xyznumList)
println(xyznumList.head)
println(xyznumList.tail,"tail")
println(xyznumList.isEmpty)
println(xyznumList.reverse)
var sum=0
//def sum: xyznumList
//def  sorted[Y >: xyznumList]: List[xyznumList]

var alist=List[Int]()

var blist=List()


var clist=List()

var abclist=List(alist,blist,clist)
println(abclist)
alist::=22
alist::=23
alist::=24
alist::=25
alist=222::alist




print(alist,"d")


//blist="df"::"rr"::"hh"::"pp"::"ss"::Nil
//var dec1=2.3
//var dec2=3.4
//clist=dec1::dec2::Nil
println(abclist)


var dm  = List[String]()
var dk = List[Map[String,AnyRef]]()

dm = "text" :: dm
dm ::= "text2"
//dk = Map(1 -> "ok") :: dk
var lsp: Unit = println("\n")

print(dm)
lsp
println(List.tabulate(5)(x=>x*x))
lsp
println(List.tabulate(6)(x=>x*x))
lsp
println(List.tabulate(10)(x=>x))
lsp
println(List.iterate(1,10)(x=> x +1))




}
