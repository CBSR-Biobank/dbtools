import scala.slick.driver.MySQLDriver.simple._
import scala.slick.jdbc.StaticQuery
import org.joda.time.DateTime
import com.github.nscala_time.time.Imports._

object SpecimenWebtable extends Command {

  val Name = "webtable"

  val Help =
    s"""|Recreates the specimen webtable. This table is used to export specimen information from
        |Biobank to other applications. if the argument \"dropfirst\" is used, the table will first
        |be dropped before it is recreated. """.stripMargin

  val Usage = s"$Name [dropfirst]"

  val CreateTable =
    s"""|CREATE TABLE IF NOT EXISTS `specimen_webtable` (
        |  `id` int(11) NOT NULL AUTO_INCREMENT,
        |  `INVENTORY_ID` varchar(100) CHARACTER SET latin1 COLLATE latin1_general_cs DEFAULT NULL,
        |  `SPECIMEN_TYPE` varchar(255) CHARACTER SET latin1 COLLATE latin1_general_cs DEFAULT NULL,
        |  `SINVENTORY_ID` varchar(100) CHARACTER SET latin1 COLLATE latin1_general_cs DEFAULT NULL,
        |  `STUDY` varchar(50) CHARACTER SET latin1 COLLATE latin1_general_cs DEFAULT NULL,
        |  `PNUMBER` varchar(100) CHARACTER SET latin1 COLLATE latin1_general_cs DEFAULT NULL,
        |  `VNUMBER` int(11) DEFAULT NULL,
        |  `CREATED_AT` datetime DEFAULT NULL,
        |  `CENTER` varchar(255) CHARACTER SET latin1 COLLATE latin1_general_cs DEFAULT NULL,
        |  `POS_LABEL` varchar(255) CHARACTER SET latin1 COLLATE latin1_general_cs DEFAULT NULL,
        |  `QUANTITY` decimal(20,10) DEFAULT NULL,
        |  `TOP_CONTAINER` varchar(255) CHARACTER SET latin1 COLLATE latin1_general_cs DEFAULT NULL,
        |  `COMMENT` text CHARACTER SET latin1 COLLATE latin1_general_cs,
        |  PRIMARY KEY (`id`)
        |) ENGINE=InnoDB  DEFAULT CHARSET=latin1 AUTO_INCREMENT=1""".stripMargin

  // For now inserts NULLs for comments
  val InsertIntoTable =
    s"""|INSERT INTO specimen_webtable (
        |   INVENTORY_ID, SPECIMEN_TYPE, SINVENTORY_ID, STUDY, PNUMBER, VNUMBER, CREATED_AT, CENTER,
        |   POS_LABEL, QUANTITY, TOP_CONTAINER, COMMENT)
        |SELECT spc.inventory_id, stype.name, topspc.inventory_id, study.name_short, pt.pnumber,
        |   ce.visit_number, spc.created_at, center.name_short,
        |   CONCAT(cntr.label, spos.position_string),  spc.quantity, top_cntr_type.name_short, null
        |FROM specimen spc
        |LEFT JOIN specimen topspc ON topspc.id=spc.top_specimen_id
        |JOIN specimen_type stype ON stype.id=spc.specimen_type_id
        |JOIN collection_event ce ON ce.id=spc.collection_event_id
        |JOIN patient pt ON pt.id=ce.patient_id
        |JOIN study ON study.id=pt.study_id
        |JOIN center ON center.id=spc.current_center_id
        |LEFT JOIN specimen_position spos ON spos.specimen_id=spc.id
        |LEFT JOIN container cntr ON cntr.id=spos.container_id
        |LEFT JOIN container top_cntr ON top_cntr.id=cntr.top_container_id
        |LEFT JOIN container_type top_cntr_type ON top_cntr_type.id=top_cntr.container_type_id
        |#LEFT JOIN specimen_comment spc_cmnt ON spc_cmnt.specimen_id=spc.id
        |#LEFT JOIN comment ON comment.id=spc_cmnt.comment_id
        |WHERE spc.activity_status_id=1
        |AND topspc.id is not NULL
        |AND (cntr.label is NULL OR cntr.label not like 'SS%')""".stripMargin

  val TruncateTable = "TRUNCATE specimen_webtable"

  val DropTable = "DROP TABLE specimen_webtable"

  def invokeCommand(args: Array[String])(implicit session: Session): Unit = {
    args.length match {
      case 1 if (args(0) == "dropfirst") =>
        StaticQuery.updateNA(DropTable).execute
        println("Dropping specimen webtable before recreating it")
      case _ =>
        println(s"\tError: invalid argument(s): ${args.mkString(", ")}")
        println(s"\nusage: $Usage")
        System.exit(1)
    }

    println("creating specimen web table")
    val start = DateTime.now
    StaticQuery.updateNA(CreateTable).execute
    StaticQuery.updateNA(TruncateTable).execute
    StaticQuery.updateNA(InsertIntoTable).execute
    println("...done.")
    val executionTimeSecs = (start to DateTime.now).millis / 1000
    println(s"update took $executionTimeSecs seconds")
  }
}
