 package com.priyam.spark

import org.apache.log4j._
import org.apache.spark._

/** Count up how many of each star rating exists in the MovieLens 100K data set. */
object custOrder {

  /** Our main function where the action happens */
  def main(args: Array[String]) {

    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)

    // Create a SparkContext using every core of the local machine, named RatingsCounter
    val sc = new SparkContext("local[*]", "customer-orders")

    // Load up each line of the ratings data into an RDD
    val lines = sc.textFile("../ml-100k/customer-orders.csv")
    val rdd1=lines.map(parseLine)
    val rdd2=rdd1.reduceByKey((x,y)=>x+y)
    val rdd3=rdd2.map(x=>(x._2,x._1)).sortByKey()
      rdd3.collect().foreach(println)
  }
  def parseLine(line:String)={
    val fields=line.split(",")
    val name=fields(0).toInt
    val numFrnds=fields(2).toFloat
    (name,numFrnds)
  }
}


 