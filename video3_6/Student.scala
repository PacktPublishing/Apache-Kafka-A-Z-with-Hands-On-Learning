package com.kafka.scala.demo2.subdemo1

class Student {
  private var name: String = ""
  private var age: Int = 0

  def this(name: String, age: Int) {
    this()
    this.name = name
    this.age = age
  }

  def getName: String = this.name

  def getAge: Int = this.age

  override def toString: String = "User(" + name + ", " + age + ")"

}
