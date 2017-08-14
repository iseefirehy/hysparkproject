package com.imooc.log

import org.apache.spark.sql.{SaveMode, SparkSession}

/**
  * Created by zhanghongyu on 8/12/17.
  * 使用spark完成数据清洗操作
  */
object SparkStatCleanJob {

  def main(args: Array[String]) {

    val spark = SparkSession.builder().appName("SparkStatCleanJob").master("local[2]").getOrCreate()

    ///Users/zhanghongyu/Documents/LogAnalysis/access.log

    val accessRDD = spark.sparkContext.textFile("/Users/zhanghongyu/Documents/LogAnalysis/access.log")

    //accessRDD.take(10).foreach(println)

    //RDD到DF

    val  accessDF = spark.createDataFrame(accessRDD.map(x => AccessConvertUtilis.parseLog(x)),AccessConvertUtilis.struct)

//    accessDF.printSchema()
//    accessDF.show(false)
      accessDF.coalesce(1).write.format("parquet").mode(SaveMode.Overwrite).partitionBy("day").save("/Users/zhanghongyu/Documents/LogAnalysis/clean")
    spark.stop
  }

}
