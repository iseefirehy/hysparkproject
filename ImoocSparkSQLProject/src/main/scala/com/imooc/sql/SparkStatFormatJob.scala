package com.imooc.sql

import org.apache.spark.sql.SparkSession

/**
  * Created by zhanghongyu on 8/11/17.
  * 第一步清洗抽取出我们所需要的指定列的数据
  */
object SparkStatFormatJob {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("SparkStatFormatJob").master("local[2]").getOrCreate()


    val access = spark.sparkContext.textFile("file:///Users/zhanghongyu/Documents/Spark SQL/data/10000_access.log")

    //access.take(10).foreach(println)

    access.map(line=>{
      val splits = line.split(" ")
      val ip = splits(0)

      /*
      * 原始日志的第三个和第四个字段拼接起来就是完整的访问时间：[10/Nov/2016:00:01:02 +0800]
      * ==>yyyy-MM-dd HH:mm:ss
      * */
      val time = splits(3) + " " + splits(4)
      val url = splits(11).replaceAll("\"","")
      val traffic = splits(9)

      //(ip,DateUtils.parse(time),url,traffic)

      DateUtils.parse(time) + "\t" + url + "\t"+traffic + "\t" + ip
    }).saveAsTextFile("file:///Users/zhanghongyu/Documents/Spark SQL/data/output/")


    spark.stop()
  }
}
