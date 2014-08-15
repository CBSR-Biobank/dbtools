import scala.slick.driver.MySQLDriver.simple._

trait Command {

  val Name: String

  val Help: String

  val Usage: String

  def invokeCommand(args: Array[String])(implicit session: Session): Unit

}
