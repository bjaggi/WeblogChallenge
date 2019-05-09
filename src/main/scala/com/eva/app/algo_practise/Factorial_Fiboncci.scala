package com.eva.app.algo_practise

object Factorial_Fiboncci {

  def main(args: Array[String]): Unit = {


    println(fibonacci(4))
  }

  def fibonacci(i: Int): Int = {

   if ( i == 1)
     1
    else
     fibonacci(i-1) * i

    }

    // return fibonacci(i)*fibonacci(i-1)


}