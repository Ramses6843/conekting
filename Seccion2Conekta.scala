package exercises

import Array._
object Seccion2Conekta extends App {

  val cien = range(1,101)
  val num = (args mkString ", ").toInt

  new Calculate(num, cien)

}

class Calculate(num: Int, cien: Array[Int]){

  var nullString: String = _

  if(num <= 100){
    Extract(num)
  }else{
    println("El numero " + num + " es mayor a 100, es invalido")
  }

  def Extract(num: Int): Unit = {
    var num_extrated: Boolean = false

    for ( i <- cien )
    {
      num_extrated = num match {
        case i => true
        case _ => false
      }
    }

    num_extrated match {
      case true => println("El número extraido es: " + num)
      case _ => println("El numero " + num + " es invalido")
    }

  }

}

