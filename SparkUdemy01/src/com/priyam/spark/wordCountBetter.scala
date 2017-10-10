package com.priyam.spark
import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.log4j._

/** Count up how many of each word occurs in a book, using regular expressions. */
object WordCountBetter {

  /** Our main function where the action happens */
  def main(args: Array[String]) {

    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)

    // Create a SparkContext using every core of the local machine
    val sc = new SparkContext("local[*]", "WordCountBetter")

    // Load each line of my book into an RDD
    val input = sc.textFile("../ml-100k/book.txt")

    // Split using a regular expression that extracts words
    val words = input.flatMap(x => x.split("\\W+"))
   // words.filter(x=> x=="you")
    // Normalize everything to lowercase
   val lowercaseWords = words.map(x => x.toLowerCase()).filter(x=> x =="you")

    // Count of the occurrences of each word
    //val wordCounts = lowercaseWords. countByValue()
    val wordCounts = lowercaseWords.map(x=>(x,1)).reduceByKey((x,y)=>x+y).foreach(println)

  //  val wcsorted=wordCounts.map(x=>(x._2,x._1))
   // wordCounts.collect
    // Print the results
    //wordCounts.foreach(println)

//    for(r <- wcsorted){
//      val w=r._2
//      val c=r._1
//      println(s"$w: $c")
//    }

  }

}
 