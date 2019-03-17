package com.ibhale.spark.scala
import org.apache.spark.sql.SQLContext
// General spark sql operations
object SparkSqlOps {

  def execute(spark: SQLContext) {
    println("starting spark sql operations ")

    //read sample parquet file
    val userData = spark.read.parquet("./src/main/resources/userdata.parquet")

    //print 10 rows
    userData.take(10)
    print(userData.count())

    //generic sql operations with clauses select, where, etc
    userData.selectExpr("id", "country").show()
    userData.where(userData("id") < 20).show
    userData.orderBy("country").take(10).foreach(x => {
      println(x.toString())
    })

  }

}