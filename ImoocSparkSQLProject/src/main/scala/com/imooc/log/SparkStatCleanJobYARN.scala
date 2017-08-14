package com.imooc.log

import org.apache.spark.sql.{SaveMode, SparkSession}

/**
  * Created by zhanghongyu on 8/12/17.
  * 使用spark完成数据清洗操作:运行在YARN上
  */
object SparkStatCleanJobYARN {

  def main(args: Array[String]) {

    if(args.length != 2){
      println("Usage:SparkStatCleanJobYARN <inputPath><OutputPath>")
      System.exit(1)
    }

    val Array(inputPath,outputPath) = args

    val spark = SparkSession.builder().getOrCreate()

    ///Users/zhanghongyu/Documents/LogAnalysis/access.log

    val accessRDD = spark.sparkContext.textFile(inputPath)



    //RDD到DF

    val  accessDF = spark.createDataFrame(accessRDD.map(x => AccessConvertUtilis.parseLog(x)),AccessConvertUtilis.struct)


      accessDF.coalesce(1).write.format("parquet").mode(SaveMode.Overwrite).partitionBy("day").save(outputPath)
    spark.stop
  }

}
