package com.imooc.log

import java.sql.{Connection, DriverManager, PreparedStatement}


/**
  * Created by zhanghongyu on 8/12/17.
  * MySQL操作工具类
  *
  */
object MySQLUtils {
  /*
  * 获取数据库连接
  * */

  def getConnection()={
    DriverManager.getConnection("jdbc:mysql://localhost:3306/logproject?user=root&password=200396")
  }

  /*
  * 释放数据库连接等资源
  *
  * */
  def release(conneciton:Connection,pstmt:PreparedStatement)={
    try{
        if(pstmt != null){
          pstmt.close()
        }
    }catch {
      case e:Exception => e.printStackTrace()
    }finally {

    }
  }
  def main(args: Array[String]) {
    println(getConnection())
  }
}
