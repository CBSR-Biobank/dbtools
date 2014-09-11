package org.biobank.dbtools

import com.typesafe.config.ConfigFactory
import scala.slick.driver.MySQLDriver.simple._
import scala.collection.mutable.Map

object DbTools {

  def main(args: Array[String]) = {
    implicit val session = DbConfig.session

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

    Commands.invokeCommand(commandName, args.slice(1, args.length))
  }

  def addCommands = {
    Commands.addCommand(SpecimenWebtable)
    Commands.addCommand(KdcsSpecimenPull)
  }
}
