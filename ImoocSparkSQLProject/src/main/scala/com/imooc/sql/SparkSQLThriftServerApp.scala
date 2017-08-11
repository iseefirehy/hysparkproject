package com.imooc.sql

import java.sql.DriverManager

/**
  * 通过jdbc方式访问
  */
object SparkSQLThriftServerApp {
  def main(args: Array[String]) {
    Class.forName("org.apache.hive.jdbc.HiveDriver")
    val conn = DriverManager.getConnection("jdbc:hive2://localhost:10000","localhost","")
    val pstmt = conn.prepareStatement("select empno,ename,sal from emp")
    val rs = pstmt.executeQuery()
    while (rs.next()){
      println("empno:" + rs.getInt("empno") + ",ename:" + rs.getString("ename") + ",sal:" + rs.getDouble("sal"))
    }
    rs.close()
    pstmt.close()
    conn.close()

   }
}
