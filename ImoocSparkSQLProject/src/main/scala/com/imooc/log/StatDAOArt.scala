package com.imooc.log

import java.sql.{Connection, PreparedStatement}

import scala.collection.mutable.ListBuffer

/**
  * Created by zhanghongyu on 8/12/17.
  * 各个维度统计的DAO操作 Article的
  */
object StatDAOArt {

  /*
  * 批量保存DayVideoAccessStat到数据库
  *
  * */
  def insertDayArticleAccessTopN(list:ListBuffer[DayArticleAccessStat]):Unit = {
    var connection:Connection = null
    var pstmt:PreparedStatement = null

    try{
      connection = MySQLUtils.getConnection()

      connection.setAutoCommit(false)//设置自动提交

      val sql = "insert into day_article_access_topn_stat(day,cms_id,times)values(?,?,?)"
      pstmt = connection.prepareStatement(sql)

      for(ele <- list){
        pstmt.setString(1,ele.day)
        pstmt.setLong(2,ele.cmsId)
        pstmt.setLong(3,ele.times)

        pstmt.addBatch()
      }

      pstmt.executeBatch() //执行批量处理
      connection.commit() //手工提交
    }catch {
      case e:Exception=> e.printStackTrace()

    }finally {
      MySQLUtils.release(connection, pstmt)
    }
  }
}
