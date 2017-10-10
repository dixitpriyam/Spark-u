package com.priyam.spark

import org.apache.log4j._
import org.apache.spark._

/** Count up how many of each star rating exists in the MovieLens 100K data set. */
object wordCount {
 
  /** Our main function where the action happens */
  def main(args: Array[String]) {
   
    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)
       
    // Create a SparkContext using every core of the local machine, named RatingsCounter
    val sc = new SparkContext("local[*]", "RatingsCounter")
   
    // Load up each line of the ratings data into an RDD
    val lines = sc.textFile("../ml-100k/book.txt")

    lines.flatMap(x=>x.split(" ")).countByValue().foreach(println)



  }
}
 