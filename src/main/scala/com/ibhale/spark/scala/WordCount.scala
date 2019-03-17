package com.ibhale.spark.scala
import org.apache.spark._

// Word count using map reduce
object WordCount{
  
  def executeWordCount(sc:SparkContext)
  {
    println("starting word count process ")
    
    //create RDD from input file
    val test = sc.textFile("./src/main/resources/input.txt")

    test.flatMap { line => //separate lines
      line.split(" ") //delimiter for word
    }
      .map { word =>(word, 1) } //count words
      .reduceByKey(_ + _) //compute sum of same words
    //  .saveAsTextFile("./src/main/resources/output.txt") //Save to a text file
    
    
    
  }
  
}