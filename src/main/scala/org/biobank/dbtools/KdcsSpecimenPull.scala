package org.biobank.dbtools

import scala.collection.mutable.ListBuffer
import com.github.tototoshi.csv._
import org.slf4j.LoggerFactory
import com.typesafe.scalalogging.Logger
import java.io.FileNotFoundException
import scala.slick.driver.MySQLDriver.simple._
import scala.slick.jdbc.{GetResult, StaticQuery => Q}
import Q.interpolation
import com.typesafe.config.ConfigFactory
import org.joda.time.DateTime
import org.joda.time.format.DateTimeFormat
import com.github.tototoshi.slick.MySQLJodaSupport._
import com.github.nscala_time.time.Imports._
import scala.util.{ Try, Success, Failure }

/** Tool that helps with a CBSR specimen pull.
  *
  * On 2014-08-07, Aaron Peck asked for some help with a specimen pull from KDCS. The patient
  * information and date drawn are read from a CSV file. Two CSV files are generated. Since the date
  * drawn may not be accurate, specimens with a date drawn within 5 days before or after are
  * considered.
  *
  * The first CSV file lists all specimens for the patient and date drawn.  The second CSV file
  * gives a count of the valid specimen types found for the patient and the date drawn.
  *
  * Some of the specimens were already pulled and present in the SS container. The query on the
  * database takes this into account.
  *
  * On 2014-09-11, Aaron asked for the tool to help pull Plasma specimen types. The CSV file for
  * plasma now has a count of the number of specimens to pull.
  */
object KdcsSpecimenPull extends Command {

  val Name = "kdcspull"

  val Help = s"""
    |Helps with a CBSR specimen pull for KDCS study. Patient information comes from file CSVFILE.
    |SPCTYPES can be either "serum" or "plasma" and where "serum" is default if not specified.
    |""".stripMargin

  val Usage = s"$Name CSVFILE SPCTYPES"

  val specimensFilename = "specimens.csv"
  val spcTypeCountFilename = "spcTypeCounts.csv"

  def invokeCommand(args: Array[String])(implicit session: Session) = {
    if (args.size < 1) {
      println("\tError: no arguments file specified")
      System.exit(1)
    } else if (args.size > 2) {
      println("\ttoo many paramters")
      System.exit(1)
    }

    val specimenTypes = if (args.size == 2) { args(1) } else { "serum" }

    val csvInputFilename = args(0)

    Try(CSVReader.open(csvInputFilename)) match {
      case Success(csvReader) =>
        val specimensCsvWriter = CSVWriter.open(specimensFilename)
        val spcTypeCountCsvWriter = CSVWriter.open(spcTypeCountFilename)

        new KdcsSpecimenPull(csvReader, specimenTypes, specimensCsvWriter, spcTypeCountCsvWriter)

        csvReader.close()
        specimensCsvWriter.close()
        spcTypeCountCsvWriter.close()

      case Failure(ex) =>
        println(s"CSV file error: ${ex.getMessage}")
    }
  }
}

/**
  * @param csvReader Where the csvReader is read from.
  *
  * @param specimensCsvWriter Where the specimensCsvWriter will be saved.
  */
class KdcsSpecimenPull(
  csvReader: CSVReader,
  specimenTypes: String,
  specimensCsvWriter: CSVWriter,
  spcTypeCountCsvWriter: CSVWriter)(implicit session: Session) {

  val log = Logger(LoggerFactory.getLogger(this.getClass))

  val DateFormat = DateTimeFormat.forPattern("yyyy-MM-dd")

  case class Specimen(
    id: Int,
    inventoryId: String,
    quantity: BigDecimal,
    createdAt: DateTime,
    activityStatusId: Int,
    originalCollectionEventId: Int,
    processingEventId: Int,
    originInfoId: Int,
    speicmenTypeId: Int,
    collectionEventId: Int,
    parentSpecimenId: Int,
    currentCenterId: Int,
    version: Int
  )

  implicit val GetSpecimenResult = GetResult(r =>
    Specimen(r.<<, r.<<, r.<<, r.<<, r.<<, r.<<, r.<<, r.<<, r.<<, r.<<, r.<<, r.<<, r.<<))

  val ValidSsLabels = """
    |'SSBH08', 'SSBH09', 'SSBH10', 'SSBH11', 'SSBH12', 'SSBH13', 'SSBH14', 'SSBH15',
    |'SSBJ01', 'SSBJ02', 'SSBJ03', 'SSBJ04', 'SSBJ05', 'SSBJ06'""".stripMargin

  val SerumSpecimenTypes = """
    |'Serum B', 'SerumB100', 'SerumB300', 'SerumG100', 'SerumG300', 'SerumG400'
    """.stripMargin

  val PlasmaSpecimenTypes = "'Plasma'"

  def BaseQry(specimenTypes: String, validSsLabels: String) = {
    s"""
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
    |WHERE pt.pnumber = ?
    |AND study.name_short = 'KDCS'
    |AND stype.name_short in ($specimenTypes)
    |AND topspc.id is not NULL
    |AND (DATEDIFF(topspc.created_at, ?) >= -5 AND DATEDIFF(topspc.created_at, ?) <= 5)
    |AND cntr.id is not NULL
    |AND (cntr.label not like 'SS%' OR cntr.label in ($validSsLabels))
    |AND spc.activity_status_id = 1""".stripMargin
  }

  def SpecimenQry(baseQry: String) = {
    s"""
    |SELECT spc.id, spc.inventory_id, stype.name_short, topspc.inventory_id, pt.pnumber,
    |   ce.visit_number, topspc.created_at, spc.quantity, center.name_short, cntr.label,
    |   spos.position_string, top_cntr_type.name_short
    |$baseQry""".stripMargin
  }

  def SpecimenTypeCountQry(baseQry: String) = {
    s"""
    |SELECT pt.pnumber, ce.visit_number, topspc.created_at, stype.name_short, count(*)
    |$baseQry
    |GROUP BY stype.name_short""".stripMargin
  }

  // used to keep track of errors
  val errors = new ListBuffer[String]

  def getSpecimenInfo(
    pnumber: String,
    dateStr: String,
    specimenTypes: String,
    count: Option[Int] = None) = {

    val baseQry = BaseQry(specimenTypes, ValidSsLabels)
    val qry = Q.query[(String, String, String), SpecimenDetails](SpecimenQry(baseQry))

    val specimens = qry((pnumber, dateStr, dateStr)).list

    if (specimens.isEmpty) {
      errors += s"no specimens found for patient: patient: $pnumber, date: $dateStr"
    } else {
      val numToTake = count.getOrElse(specimens.size)

      specimens.take(numToTake).foreach { specimen =>
        log.debug(s"result: $specimen")

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

        specimensCsvWriter.writeRow(row)
      }

      // write empty row to make easier to read
      specimensCsvWriter.writeRow(List())
    }
  }

  def getSpecimenCountInfo(
    pnumber: String,
    dateStr: String,
    specimenTypes: String) = {
    case class SpecimenTypeCount(
      pnumber: String,
      visitNumber: Int,
      dateDrawn: DateTime,
      specimenTypeName: String,
      count: Int)

    implicit val getSpecimenTypeCountResult = GetResult(r =>
      SpecimenTypeCount(r.<<, r.<<, r.<<, r.<<, r.<<))

    val baseQry = BaseQry(specimenTypes, ValidSsLabels)
    val qry = Q.query[(String, String, String), SpecimenTypeCount](SpecimenTypeCountQry(baseQry))

    val specimenCounts = qry((pnumber, dateStr, dateStr)).list

    specimenCounts.foreach{ specimenCount =>
      log.debug(s"result: $specimenCount")

      val row = List(
        specimenCount.pnumber,
        specimenCount.visitNumber,
        DateFormat.print(specimenCount.dateDrawn),
        specimenCount.specimenTypeName,
        specimenCount.count
      )

      spcTypeCountCsvWriter.writeRow(row)
    }

    spcTypeCountCsvWriter.writeRow(List())
  }

  specimensCsvWriter.writeRow(List(
    "pnumber",
    "visitNumber",
    "dateDrawn",
    "inventoryId",
    "specimenType",
    "centre",
    "label",
    "topContainerType"
  ))

  spcTypeCountCsvWriter.writeRow(List(
    "pnumber",
    "visitNumber",
    "dateDrawn",
    "specimenType",
    "specimenCount"
  ))

  specimenTypes match {
    case "serum" => {
      csvReader.allWithHeaders.foreach{ csvRow =>
        //log.debug(s"$csvRow")
        getSpecimenInfo(csvRow("StudyNum"), csvRow("lbCollectionDate"), SerumSpecimenTypes)
        getSpecimenCountInfo(csvRow("StudyNum"), csvRow("lbCollectionDate"), SerumSpecimenTypes)
      }
    }
    case "plasma" => {
      csvReader.allWithHeaders.foreach{ csvRow =>
        //log.debug(s"$csvRow")
        getSpecimenInfo(
          csvRow("StudyNum"),
          csvRow("lbCollectionDate"),
          PlasmaSpecimenTypes,
          Some(csvRow("Count").toInt))
        getSpecimenCountInfo(csvRow("StudyNum"), csvRow("lbCollectionDate"), PlasmaSpecimenTypes)
      }
    }
  }


  // display errors in file
  if (!errors.isEmpty) {
    specimensCsvWriter.writeRow(List())
    errors.toList.foreach{ msg =>
      log.error(msg)
      specimensCsvWriter.writeRow(List(msg))
    }
  }

}
