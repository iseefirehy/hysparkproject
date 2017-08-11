package com.imooc.sql

import org.apache.spark.sql.SparkSession

/**
  * Created by zhanghongyu on 8/10/17.
  * 使用外部数据源综合查询Hive和MySQL的表数据
  */
object HiveMySQLApp {

  def main(args: Array[String]) {
    val spark = SparkSession.builder().appName("HiveMySQLApp").master("local[2]").getOrCreate()

    //加载Hive表数据
    val hiveDF = spark.table("t1.emp")
    //加载MySQL的数据
    val mySQLDF = spark.read.format("jdbc").option("url", "jdbc:mysql://localhost:3306").option("dbtable", "spark.DEPT").option("user", "root").option("password", "200396").option("driver", "com.mysql.jdbc.Driver").load()
    //JOIN
    val resultDF = hiveDF.join(mySQLDF,hiveDF.col("deptno") === mySQLDF.col("DEPTNO"))

    resultDF.show

    spark.stop()
  }

}
