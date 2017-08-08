package com.imooc.sql

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.hive.HiveContext

/**
  * Created by zhanghongyu on 8/7/17.
  * HiveContext的使用
  */
object HiveContextApp {
  def main(args: Array[String]) {
    val sparkConf = new SparkConf()
    //在测试或者生产中，APPNAME和Master我们是通过脚本进行指定
    //sparkConf.setAppName("HiveContextAPP").setMaster("local[2]")
    val sc = new SparkContext(sparkConf)
    val hiveContext = new HiveContext(sc)

    //2)相关处理 : json文件
    hiveContext.table("emp").show
    //3)关闭资源
    sc.stop()
  }

}
