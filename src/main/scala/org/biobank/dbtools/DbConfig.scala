package org.biobank.dbtools

import com.typesafe.config._
import scala.slick.jdbc.JdbcBackend.Database

object DbConfig {

  case class DbConfigParams(host: String, name: String, user: String, password: String)

  val ConfigResourceName = "db"

  val ConfigPath = "db"

  val conf = ConfigFactory.load(ConfigResourceName)

  if (!conf.hasPath(ConfigPath)) {
    println(s"\tError: settings not found in ${ConfigResourceName}.conf")
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
