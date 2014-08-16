import com.typesafe.config.ConfigFactory
import scala.slick.driver.MySQLDriver.simple._
import scala.collection.mutable.Map

object DbTools {

  case class AppConfig(host: String, name: String, user: String, password: String)

  val ConfigResourceName = "db"

  val ConfigPath = "db"

  lazy val appConfig: AppConfig = getConfig

  def main(args: Array[String]) = {
    println(s"${buildinfo.BuildInfo.name} version: ${buildinfo.BuildInfo.version}")
    addCommands

    if (args.size < 1) {
      println("Error: command not specified.\n")
      Commands.showCommands
      System.exit(1)
    }

    val commandName = args(0)

    if (commandName == "help") {
      if (args.size == 1) {
        Commands.showCommandsAndHelp
        System.exit(0)
      } else if (args.size == 2) {
        Commands.showCommandHelp(args(1))
        System.exit(0)
      } else  {
        println("\tError: invalid command")
        System.exit(1)
      }
    }

    appConfig

    Database.forURL(
      s"jdbc:mysql://${appConfig.host}:3306/${appConfig.name}",
      driver   = "com.mysql.jdbc.Driver",
      user     = appConfig.user,
      password = appConfig.password).withSession { implicit session =>

      Commands.invokeCommand(commandName, args.slice(1, args.length))
    }
  }

  def getConfig: AppConfig = {
    val conf = ConfigFactory.load(ConfigResourceName)

    if (!conf.hasPath(ConfigPath)) {
      println(s"\tError: settings not found in ${ConfigResourceName}.conf")
      System.exit(1)
    }

    val dbConf = conf.getConfig(ConfigPath);
    AppConfig(
      dbConf.getString("host"),
      dbConf.getString("name"),
      dbConf.getString("user"),
      dbConf.getString("password"))
  }

  def addCommands = {
    Commands.addCommand(SpecimenWebtable)
    Commands.addCommand(KdcsSpecimenPull)
  }
}
