package com.imooc.sql

import org.apache.spark.sql.SparkSession

/**
  * Created by zhanghongyu on 8/10/17.
  * Parquet文件操作
  */
object ParquetApp {
  def main(args: Array[String]) {
    val spark = SparkSession.builder().appName("SparkSessionApp").master("local[2]").getOrCreate()
    /**
      * spark.read.format("parquet").load这是标准写法
      */

    val userDF = spark.read.format("parquet").load("file:///Users/zhanghongyu/Documents/Hive/ImoocSparkSQLProject/users.parquet")

    userDF.printSchema()
    userDF.show()

    userDF.select("name","favorite_color").show

    userDF.select("name","favorite_color").write.format("json").save("file:///Users/zhanghongyu/Documents/Hive/ImoocSparkSQLProject/jsonout")

    spark.read.load("file:///Users/zhanghongyu/Documents/Hive/ImoocSparkSQLProject/users.parquet").show
    //会报错，因为sparksql默认处理的format就是parquet
    spark.read.load("file:///Users/zhanghongyu/Documents/Hive/ImoocSparkSQLProject/people.json").show

    spark.sqlContext.setConf("spark.sql.shuffle.partitions","10")

    spark.read.format("parquet").option("path","file:///Users/zhanghongyu/Documents/Hive/ImoocSparkSQLProject/users.parquet").load().show
    spark.stop()
  }

}
