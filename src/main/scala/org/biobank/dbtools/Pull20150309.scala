package org.biobank.dbtools

import scala.slick.driver.MySQLDriver.simple._
import scala.slick.jdbc.{ GetResult, StaticQuery => Q }
import Q.interpolation
import org.joda.time.DateTime
import org.joda.time.format.DateTimeFormat
import scala.slick.driver.MySQLDriver.simple._
import com.github.tototoshi.slick.MySQLJodaSupport._
import com.github.nscala_time.time.Imports._

object Pull20150309 extends Command {

  val Name = "pull20150309"

  val Help =
    s"""|Pull requested by Aaron. """.stripMargin

  val Usage = s"$Name"

  val patients = List(
    "UB0078",
    "UB0079",
    "UB0083",
    "UB0073",
    "UB0074",
    "UB0080",
    "UB0103",
    "UB0110",
    "UB0007",
    "UB0048",
    "UB0014",
    "UB0037",
    "UB0111",
    "UB0103",
    "UB0092",
    "UB0086",
    "UB0090",
    "UB0064",
    "UB0065",
    "UB0026",
    "UB0131",
    "UB0061",
    "UB0002",
    "UB0047",
    "UB0132",
    "UB0053",
    "UB0072",
    "UB0109",
    "UB0066",
    "UB0067",
    "UB0068",
    "UB0010",
    "UB0025",
    "UB0041",
    "UB0054",
    "UB0022",
    "UB0075",
    "UB0081"
  )

  val CreateTable =
    s"""|CREATE TEMPORARY TABLE IF NOT EXISTS `pull_patient` (
        |  `id` int(11) NOT NULL AUTO_INCREMENT,
        |  `PNUMBER` varchar(100) CHARACTER SET latin1 COLLATE latin1_general_cs DEFAULT NULL,
        |  PRIMARY KEY (`id`)
        |) ENGINE=InnoDB  DEFAULT CHARSET=latin1 AUTO_INCREMENT=1""".stripMargin

  def BaseQry(specimenTypes: String) = {
    s"""
    |SELECT spc.id, spc.inventory_id, stype.name_short, topspc.inventory_id, pt.pnumber,
    |   ce.visit_number, topspc.created_at, spc.quantity, center.name_short, cntr.label,
    |   spos.position_string, top_cntr_type.name_short
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
    |JOIN pull_patient pp on pp.pnumber=pt.pnumber
    |WHERE stype.name_short in ($specimenTypes)
    |AND ce.visit_number=1
    |AND topspc.id is not NULL
    |AND cntr.id is not NULL
    |AND (cntr.label not like 'SS%')
    |AND spc.activity_status_id = 1
    |ORDER BY pt.pnumber,topspc.created_at""".stripMargin
  }

  val DateFormat = DateTimeFormat.forPattern("yyyy-MM-dd")

  def invokeCommand(args: Array[String])(implicit session: Session): Unit = {
    if (args.length != 0) {
      println(s"\tError: no arguments required")
      println(s"\nusage: $Usage")
      System.exit(1)
    } else {
      Q.updateNA(CreateTable).execute

      patients.foreach(insertPullPatient)

      Q.queryNA[SpecimenDetails](BaseQry("'PlasmaL1000'")) foreach { specimen =>

        val row = List(
          specimen.pnumber,
          specimen.visitNumber,
          DateFormat.print(specimen.dateDrawn),
          specimen.inventoryId,
          specimen.specimenTypeName,
          specimen.centreName,
          specimen.containerLabel + specimen.specimenPos,
          specimen.topContainerTypeName
        )

        println(row.mkString(", "))
      }
    }
  }

  def insertPullPatient(pnumber: String)(implicit session: Session) = {
    (Q.u + s"INSERT INTO pull_patient (pnumber) VALUES ('$pnumber')").execute
  }

}
