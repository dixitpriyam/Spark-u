package com.priyam.spark

import breeze.linalg.min
import org.apache.log4j._
import org.apache.spark._

/** Count up how many of each star rating exists in the MovieLens 100K data set. */
object minMax {

  /** Our main function where the action happens */
  def main(args: Array[String]) {

    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)

    // Create a SparkContext using every core of the local machine, named RatingsCounter
    val sc = new SparkContext("local[*]", "MIN MAX")

    // Load up each line of the ratings data into an RDD
    val lines = sc.textFile("../ml-100k/1800.csv")
    val rdd=lines.map(parseLine)
    val prepFilter=rdd.filter(x=>x._2 !="PRCP")

    //val rdd2=prepFilter.map(x=> (x._1,(x._2,x._3)))
    val rdd2=prepFilter.map(x=> ((x._1,x._2),x._3))
    val rdd3=rdd2.reduceByKey((x,y)=>min(x,y))
    rdd3.foreach(println)





    //PrepFilter.sortBy(x=> x._2).foreach(println)

   // rdd.foreach(println)
    //val rdd2=rdd.mapValues(x=>(x,1)).reduceByKey((x,y)=>(x._1+y._1,x._2+y._2))
    //val rdd3=rdd2.mapValues((x=> x._1/x._2))
  // val rdd2=rdd.mapValues(x=> (x,1));

   //rdd2.sortByKey().foreach(println)
    //rdd3.sortByKey()foreach(println)
  }
  def parseLine(line:String)={
    val fields=line.split(",")
    val stationID=fields(0)
    val tempStyle=fields(2)
    val temp=fields(3).toInt
    (stationID,tempStyle,temp)
  }
}


 