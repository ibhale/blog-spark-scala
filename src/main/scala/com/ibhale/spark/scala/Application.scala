package com.ibhale.spark.scala
import org.apache.spark._
import org.apache.spark.sql.SQLContext
object Application {
  def main(args: Array[String]) = {
    System.setProperty("hadoop.home.dir", "C:/Ishan/study/softwares/");
    //Start the Spark context
    val conf = new SparkConf()
      .setAppName("WordCount")
      .setMaster("local")
    val sc = new SparkContext(conf)
    val spark: SQLContext = new org.apache.spark.sql.SQLContext(sc)
    val userData = spark.read.parquet("C:/Users/Ishan/Desktop/userdata1.parquet");
    userData.show
    //Read some example file to a test RDD
    val test = sc.textFile("C:/Users/Ishan/Desktop/input.txt")

    test.flatMap { line => //for each line
      line.split(" ") //split the line in word by word.
    }
      .map { word => //for each word
        (word, 1) //Return a key/value tuple, with the word as key and 1 as value
      }
      .reduceByKey(_ + _) //Sum all of the value with same key
      .saveAsTextFile("C:/Users/Ishan/Desktop/output.txt") //Save to a text file

    //Stop the Spark context
    sc.stop
  }
}
  
