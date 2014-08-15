import com.typesafe.config.ConfigFactory
import scala.slick.driver.MySQLDriver.simple._

object DbTools {

  case class DbSettings(host: String, name: String, user: String, password: String)

  def main(args: Array[String]) = {
    val conf = ConfigFactory.load("db")

    if (!conf.hasPath("db")) {
      println("\tError: database settings not found in db.conf")
      System.exit(1)
    }

    val dbConf = conf.getConfig("db");
    val dbSettings = DbSettings(
      dbConf.getString("host"),
      dbConf.getString("name"),
      dbConf.getString("user"),
      dbConf.getString("password"))

    Database.forURL(
      s"jdbc:mysql://${dbSettings.host}:3306/${dbSettings.name}",
      driver   = "com.mysql.jdbc.Driver",
      user     = dbSettings.user,
      password = dbSettings.password).withSession { implicit session =>

      args(0) match {
        case "webtable" => SpecimenWebtable.createTable
        case "kdcspull" => KdcsSpecimenPull.getSpecimens(args.slice(1, args.length))
      }
    }
  }

}
