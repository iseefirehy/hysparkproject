package com.imooc.sql

import com.imooc.sql.DataFrameRDDApp.Info
import org.apache.spark.sql.SparkSession


/**
  * Created by zhanghongyu on 8/10/17.
  * dataframe中的其他操作
  */

object DataFrameCase{
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("DataFrameRDDApp").master("local[2]").getOrCreate()
    //RDD ==> DataFrame
    val rdd = spark.sparkContext.textFile("file:///Users/zhanghongyu/Documents/Hive/ImoocSparkSQLProject/student.data")
    import spark.implicits._
    val studentDF = rdd.map(_.split("\\|")).map(line => Student(line(0).toInt, line(1), line(2),line(3))).toDF()

    //show默认只显示前20条
    studentDF.show
    studentDF.show(30)
    studentDF.show(30,false)

    studentDF.take(10)

    studentDF.first()
    studentDF.select("name","email").show(30,false)

    studentDF.filter("name=''").show
    studentDF.filter("name=''OR name = 'NULL'").show

    //name以M开头的人
     studentDF.filter("SUBSTR(name,0,1)='M'").show

    studentDF.sort(studentDF("name")).show
    studentDF.sort(studentDF("name").desc).show

    studentDF.sort("name","id").show

    //名字的升序 id的降序
    studentDF.sort(studentDF("name").asc, studentDF("id").desc).show

    studentDF.select(studentDF("name").as("student_name")).show

    val studentD2F = rdd.map(_.split("\\|")).map(line => Student(line(0).toInt, line(1), line(2),line(3))).toDF()
    studentDF.join(studentD2F,studentDF.col("id")===studentD2F.col("id")).show

    spark.stop()
  }
  case class Student(id:Int,name:String,phone:String,email:String)
}
