import com.typesafe.config.ConfigFactory
import scala.slick.driver.MySQLDriver.simple._
import scala.collection.mutable.Map

object DbTools {

  case class DbSettings(host: String, name: String, user: String, password: String)

  val commands: Map[String, Command] = Map()

  def main(args: Array[String]) = {
    println(s"${buildinfo.BuildInfo.name} version: ${buildinfo.BuildInfo.version}")
    addCommands

    if (args.size < 1) {
      println("Error: command not specified.\n")
      showCommands
      System.exit(1)
    }

    val command = args(0)

    if (command == "help") {
      if (args.size == 1) {
        showCommandsAndHelp
        System.exit(0)
      } else if (args.size == 2) {
        showCommandHelp(args(1))
        System.exit(0)
      } else  {
        println("\tError: invalid command")
        System.exit(1)
      }
    }

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

      if (commands.contains(command)) {
        commands(command).invokeCommand(args.slice(1, args.length))
      }
    }
  }

  def addCommands = {
    addCommand(SpecimenWebtable)
    addCommand(KdcsSpecimenPull)
  }

  def addCommand(command: Command) = {
    commands += (command.Name -> command)
  }

  def showCommands = {
    println("Possible commands:")

    commands.values.foreach{ command =>
      println(s"\t${command.Name}")
    }
  }

  def showCommandsAndHelp = {
    println("Possible commands:\n")

    commands.values.foreach{ command =>
      println(s"${command.Name} - ${command.Help}\n")
    }
  }

  def showCommandHelp(commandName: String) = {
    if (commands.contains(commandName)) {
      val command = commands(commandName)
      println(s"usage: ${command.Usage}\n\n${command.Help}")
    } else  {
      println("invalid command: $command")
      System.exit(1)
    }
  }

}
