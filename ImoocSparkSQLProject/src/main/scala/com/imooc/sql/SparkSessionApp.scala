package com.imooc.sql

import org.apache.spark.sql.SparkSession

/**
  * Created by zhanghongyu on 8/8/17.
  */
object SparkSessionApp {
  def main(args: Array[String]) {

    val spark = SparkSession.builder().appName("SparkSession").master("local[2]").getOrCreate();


    val people = spark.read.json("file:///Users/zhanghongyu/Documents/GitHub/hysparkproject/ImoocSparkSQLProject/people.json")
    people.show()

    spark
    spark.stop()
  }
}
