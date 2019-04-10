object live{
def main(args: Array[String]) {

println("hello world")

//1) Find the last element of a list.
def listLastItem [A] (input: List[A]): A =input.last


//2) Find the last but one element of a list.
def listLastBut1Item[A](input: List[A]): A = input.init.last

//3) Find the last nth element of a list.
def listLastNthitem[A](pos: Int , input: List[A]): A = {
    if ( input.length<=0 )
            throw new IllegalArgumentException

    else if ( input.length < pos)
             throw new NoSuchElementException
    else
        input.takeRight(pos).head
}

//Find Kth element
def listKthitem[A](pos: Int , input: List[A]): A = {
if ( input.length<=0 )
throw new IllegalArgumentException

else if ( input.length < pos)
throw new NoSuchElementException
else
input(pos-1)
}

def isPalindrome[A](input: List[A] ): Boolean = { input == input.reverse }

//def flattenNextList(input: List[Any] ): List[Any] = {
//input
//}

def yourDivision(num:Int): String = {
num match {
 case t if (t >=75) =>" distinction"
 case t if (t >=60) =>" 2nd division"
 case t if (t >=40)  =>" 3rd division"
 case t if (t <40) =>" fail "
 case _ => "Wrong input"

}
}
def myMatch(e: Any): String = {
e match {
case x: String  => "Its a String: " + x
case y: Int     => "Its a Int: " + y
case x: Boolean => "Its a Boolean: " + x
case z: List[Any] => "Its a List: "
case _          => "Its something else!"
}
}

def myflatten(input: List[Any]) : List[Any] = input flatMap {
case ms: List[_] => myflatten(ms)
case e           => List(e)

}





println(myflatten(List(1,2,List(1,2,List(2,3,4,4)))))
println(myMatch(List(1,2,3)))
var myList=List(1, 2,3,4,5,6,7,8,9,10)
println(myList)
println(listLastItem(myList))
println(listLastBut1Item(myList))
println(listLastNthitem(3,myList))
println(listKthitem(3,myList))
println(isPalindrome(List(1,2,3,4,3,2,1)))
println(isPalindrome(List(1,2,3,4,4,3,2,1)))
println(yourDivision(40))


//remove duplication element without changing order

def compressList [A](input: List[A]): List[A]={
            var list=List[A]()
            var current=list
            for(z <- input)
            {
            //println("data",z,current)
            if (List(z) != current)
                    list = list:::z::Nil

             current=List(z)

            }
            list
}


def compressList2 [A](input: List[A]): List[Any]={


            var listN=List[A]()
            var  finalList=List[Any]()
            var  listY=List[A]()
            var current=listN
            for(z <- input)
            {
                    if (List(z) != current)
                        listN = listN:::z::Nil
                        if (listY.length>0)
                                {
                                finalList=List(listY)
                                listY=List()
}
                    else
                        current=List(z)
                        listY =listY:::z::Nil
                        if (listN.length>0)
                            {
                            finalList=List(listY)
                            listY=List()
                            }

            }
            finalList
}

def uniqList [A](input: List[A]): List[A]={
                        var list=List[A]()
                        var current=list
                        for(z <- input)
                            {
                                //println("data",z,current)
                                if (List(z) != current)
                                            list = list:::z::Nil

                                current=List(z)

                            }
                        list
                    }



println(compressList2(List(1,1,2,3,1,1,1,1,2,2,2,2,4,4,4,4,4,5,6,6,7,8,8,5,555,5,5,5)))

//println(compressList(List("A","A","b","b","C","d","e","d","A","A")))

//println(uniqList(List(1,1,2,3,1,1,1,1,2,2,2,2,4,4,4,4,4,5,6,6,7,8,8,5,555,5,5,5)))


}
}
