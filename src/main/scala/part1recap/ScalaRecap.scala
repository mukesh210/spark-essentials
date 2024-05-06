package part1recap

import scala.concurrent.Future
import scala.util.{Failure, Success}

object ScalaRecap extends App {
  // values and variables
  val aBoolean: Boolean = true

  // expressions
  val anIfExpression = if(2 > 3) "bigger" else "smaller"

  // instructions vs expressions
  val theUnit: Unit = println("Hello, Scala!")

  // functions
  def myFunction(x: Int) = 42

  // OOP
  class Animal
  class Cat extends Animal

  trait Carnivore {
    def eat(animal: Animal): Unit
  }

  class Crocodile extends Animal with Carnivore {
    override def eat(animal: Animal): Unit = println("Crunch!")
  }

  // Singleton Pattern
  object MySingleton
  // companion object
  object Carnivore

  // generics
  trait MyList[A]

  // method notation
  val x = 1 + 2
  val y = 1.+(2)

  // Functional Programming
  val incrementer: Function1[Int, Int] = x => x + 1
  val incremented = incrementer(42)

  // map, flatMap, filter -> Higher Order Functions
  val processedList = List(1, 2, 3).map(incrementer)

  // Pattern matching
  val unknown: Any = 45
  val ordinal: String = unknown match {
    case 1 => "first"
    case 2 => "second"
    case _ => "unknown"
  }

  // try-catch
  try {
    throw new NullPointerException
  } catch {
    case e: NullPointerException => "null pointer occured!"
    case _ => "something else"
  }

  // Future
  import scala.concurrent.ExecutionContext.Implicits.global

  val aFuture = Future {
    // some expensive computation
    42
  }
  aFuture.onComplete {
    case Success(value) => println(s"Passed with value: ${value}")
    case Failure(exception) => exception.printStackTrace()
  }

  // Partial Functions
  val aPartialFunction: PartialFunction[Int, Int] = {
    case 1 => 21
    case 8 => 23
    case _ => 999
  }
  // orElse, andThen

  // Implicits

  // auto-injection by compiler
  def methodWithImplicitArgument(implicit x: Int): Int = x + 43

  implicit val defaultInt = 2
  val implicitCall: Int = methodWithImplicitArgument

  // implicit conversions - implicit defs
  case class Person(name: String) {
    def greet = println(s"Hello ${name}!")
  }
  implicit def fromStringToPerson(name: String): Person = Person(name)

  "Bob".greet // fromStringToPerson("Bob").greet

  // implicit conversions - using classes
  implicit class Dog(name: String) {
    def bark = println("Bark!")
  }

  "Labrador".bark // Dog("Labrador").bark

  /*
    Implicit scope:
      Local Scope
      Imported Scope
      Companion objects the types involved in the method call
   */

  List(1, 2, 3).sorted
}
