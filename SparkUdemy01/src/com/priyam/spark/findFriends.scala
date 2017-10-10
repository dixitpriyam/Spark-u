package com.priyam.spark

import breeze.linalg.sum
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
    val lines = sc.textFile("../ml-100k/fakefriends.csv")
    val rdd=lines.map(parseLine)
    val rdd2=rdd.mapValues(x=>(x,1)).reduceByKey((x,y)=>(x._1+y._1,x._2+y._2))
    val rdd3=rdd2.mapValues((x=> x._1/x._2))
  // val rdd2=rdd.mapValues(x=> (x,1));

   rdd2.sortByKey().foreach(println)
    rdd3.sortByKey()foreach(println)
  }
  def parseLine(line:String)={
    val fields=line.split(",")
    val name=fields(1)
    val numFrnds=fields(3).toInt
    (name,numFrnds)
  }
}


 