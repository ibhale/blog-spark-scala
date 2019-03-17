package com.ibhale.spark.scala
import org.apache.spark._
import org.apache.spark.sql.SQLContext
object Application {
  def main(args: Array[String]) = {
   // System.setProperty("hadoop.home.dir", "./src/main/resources/");
     
    //Start the Spark context
    val conf = new SparkConf()
      .setAppName("WordCount")
      .setMaster("local")
    val sc = new SparkContext(conf)
    val spark: SQLContext = new org.apache.spark.sql.SQLContext(sc)

    // call word count method
    WordCount.executeWordCount(sc)
    
    //call spark sql operations method
    SparkSqlOps.execute(spark)

    //Stop the Spark context
    sc.stop
  }
}
  
