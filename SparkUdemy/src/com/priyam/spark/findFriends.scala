package com.priyam.spark

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.log4j._

/** Count up how many of each star rating exists in the MovieLens 100K data set. */
object findFriends {
 
  /** Our main function where the action happens */
  def main(args: Array[String]) {
   
    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)
        
    // Create a SparkContext using every core of the local machine, named RatingsCounter
    val sc = new SparkContext("local[*]", "findFriends")
   
    // Load up each line of the ratings data into an RDD
    val lines = sc.textFile("../ml-100k/fakeFriends.csv")
    
   val rdd=lines.map(parseLine)
  }
  
  
  def parseLine(line:String)={
  val fields=line.split(",")
  val age=fields(2).toInt
  val numFrnds=fields(3).toInt
  (age,numFrnds)
}
}

 