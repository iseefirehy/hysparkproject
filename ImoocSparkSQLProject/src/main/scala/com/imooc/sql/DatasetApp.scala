package com.imooc.sql

import org.apache.spark.sql.SparkSession

/**
  * Created by zhanghongyu on 8/10/17.
  * dataset 操作
  */
object DatasetApp {
  def main(args: Array[String]) {
    val spark = SparkSession.builder().appName("DatasetApp").master("local[2]").getOrCreate()

    //需要导入隐士转换
    import spark.implicits._
    val path = "file:///Users/zhanghongyu/Documents/Hive/ImoocSparkSQLProject/sales.csv"
    //spark如何解析csv文件
    val df = spark.read.option("header",true).option("inferSchema",true).csv(path)
    df.show
    //df转成ds
    val ds = df.as[Sales]
    ds.map(line => line.itemId).show
    spark.stop()
  }
  case class Sales(transactionId:Int,customerId:Int,itemId:Int,amountPaid:Double)
}
