package com.codethink.scala


/**
 * @author kelong
 * @date 9/23/15
 */
object HelloWorld {
  var factor = 3
  val multiplier = (i: Int) => i * factor;

  def main(args: Array[String]) {
    println("Hello, world!");
    var a = 30;
    if (a < 20) {
      println(a);
    }
    else {
      println("xxxxxxxxxxxxxxxxxxx");
    }
    while (a < 35) {
      println(a);
      a = a + 1;
    }
    println(add(a, 1));
    println("muliplier(1) value = " + multiplier(1))


    var li = List(1, 2, 3, 4)
    var res = li.flatMap(x => x match {
      case 3 => List(3.1, 3.2)
      case _ => List(x * 2)
    })
    println("res=========" + res)
 }

  def add(x:Int,y:Int):Int={
    var sum = 0
    sum = x+ y
    return sum
  }
}
