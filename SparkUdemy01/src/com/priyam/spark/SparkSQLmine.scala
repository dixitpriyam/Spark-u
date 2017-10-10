package com.priyam.spark

import org.apache.log4j._
import org.apache.spark._
import org.apache.spark.sql._

object SparkSQLmine {
  
  case class Person(ID:Int, name:String, age:Int, numFriends:Int)
 // val person:Person
//  def mapper(line:String): Person = {
//    val fields = line.split(',')
//
//    val person:Person = Person(fields(0).toInt, fields(1), fields(2).toInt, fields(3).toInt)
//    return person
//  }
def mapper(line: String): Person = {
  val f = line.split(",")
  val person: Person = Person(f(0).toInt, f(1), f(2).toInt, f(3).toInt)
  person
}
  /** Our main function where the action happens */
  def main(args: Array[String]) {
    
    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)
    
    // Use new SparkSession interface in Spark 2.0
    val spark = SparkSession
      .builder
      .appName("SparkSQL")
      .master("local[*]")
      .config("spark.sql.warehouse.dir", "file:///C:/temp") // Necessary to work around a Windows bug in Spark 2.0.0; omit if you're not on Windows.
      .getOrCreate()


    val lines = spark.sparkContext.textFile("../fakefriends.csv")
    val people = lines.map(mapper)
//    val people = lines.map(x=>{
//      val f=x.split(",")
//      val person:Person=Person(f(0).toInt, f(1), f(2).toInt, f(3).toInt)
//      person
//    }
//    )


//    val people = lines.map(x=>{
//      val f=x.split(",")
//      val person:Person=Person(f(0).toInt, f(1), f(2).toInt, f(3).toInt)
//      person
//    }
//    )








    // Infer the schema, and register the DataSet as a table.
    import spark.implicits._
    val schemaPeople = people.toDS
    
    schemaPeople.printSchema()
    
    schemaPeople.createOrReplaceTempView("people")
    
    // SQL can be run over DataFrames that have been registered as a table
    val teenagers = spark.sql("SELECT * FROM people WHERE age >= 13 AND age <= 19")
    
    val results = teenagers.collect()
    
    results.foreach(println)
    
    spark.stop()
  }
}