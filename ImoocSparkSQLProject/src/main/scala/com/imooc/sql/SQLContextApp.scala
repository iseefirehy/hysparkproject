package com.imooc.sql

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext

/**
  * Created by zhanghongyu on 8/7/17.
  * sqlcontext use
  * idea在本地，测试数据在服务器上，可以在本地进行开发
  */
object SQLContextApp {
  def main(args:Array[String]) ={
    //1)创建相应的Context
    val path = args(0)
    val sparkConf = new SparkConf()
    //在测试或者生产中，APPNAME和Master我们是通过脚本进行指定
    //sparkConf.setAppName("SQLContextAPP").setMaster("local[2]")
    val sc = new SparkContext(sparkConf)
    val sqlContext = new SQLContext(sc)

    //2)相关处理 : json文件
    val people = sqlContext.read.format("json").load(path)
    people.printSchema()
    people.show()
    //3)关闭资源
    sc.stop()
  }
}
