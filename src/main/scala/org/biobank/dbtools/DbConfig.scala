package org.biobank.dbtools

import com.typesafe.config._
import scala.slick.jdbc.JdbcBackend.Database
import java.io.File

/**
  * For connection pooling see:
  *
  * http://fernandezpablo85.github.io/2013/04/07/slick_connection_pooling.html
  */
object DbConfig {

  case class DbConfigParams(host: String, name: String, user: String, password: String)

  val ConfigFileName = "db.conf"

  val ConfigPath = "db"

  val conf = ConfigFactory.parseFile(new File(ConfigFileName)).resolve()

  if (!conf.hasPath(ConfigPath)) {
    println(s"\tError: settings not found in ${ConfigFileName}")
    System.exit(1)
  }

  val dbConf = conf.getConfig(ConfigPath);

  val dbConfigParams = DbConfigParams(
    dbConf.getString("host"),
    dbConf.getString("name"),
    dbConf.getString("user"),
    dbConf.getString("password"))

  val database = Database.forURL(
    s"jdbc:mysql://${dbConfigParams.host}:3306/${dbConfigParams.name}",
    driver   = "com.mysql.jdbc.Driver",
    user     = dbConfigParams.user,
    password = dbConfigParams.password)

  val session = database.createSession
}
