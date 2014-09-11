package org.biobank.dbtools

import scala.slick.driver.MySQLDriver.simple._

import scala.collection.mutable.Map

trait Command {

  val Name: String

  val Help: String

  val Usage: String

  def invokeCommand(args: Array[String])(implicit session: Session): Unit

}

object Commands {

  val commands: Map[String, Command] = Map()

  def checkCommands = {
    if (commands.isEmpty) {
      println("\tError: no commands registered")
      System.exit(1)
    }
  }

  def invokeCommand(commandName: String, args: Array[String])(implicit session: Session) = {
    checkCommands
    if (commands.contains(commandName)) {
      commands(commandName).invokeCommand(args)
    } else {
      println("\tError: invalid command: $commandName")
      System.exit(1)
    }
  }

  def addCommand(command: Command) = {
    commands += (command.Name -> command)
  }

  def showCommands = {
    checkCommands
    println("Possible commands:")

    commands.values.foreach{ command =>
      println(s"\t${command.Name}")
    }
  }

  def showCommandsAndHelp = {
    checkCommands
    println("Possible commands:\n")

    commands.values.foreach{ command =>
      println(s"${command.Name} - ${command.Help}\n")
    }
  }

  def showCommandHelp(commandName: String) = {
    checkCommands
    if (commands.contains(commandName)) {
      val command = commands(commandName)
      println(s"usage: ${command.Usage}\n\n${command.Help}")
    } else  {
      println("invalid command: $command")
      System.exit(1)
    }
  }
}
