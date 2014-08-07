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
import com.github.nscala_time.time.Imports._
import com.github.tototoshi.slick.MySQLJodaSupport._
import java.io.File

object PatientSamples {

  case class DbSettings(host: String, name: String, user: String, password: String)

  val resultsFile = "results.csv"

  def main(args: Array[String]) = {
    if (args.size < 1) {
      println("\tError: no CSV files specified")
      System.exit(1)
    } else if (args.size > 1) {
      println("\ttoo many paramters")
      System.exit(1)
    }

    val csvInputFilename = args(0)

    val output = CSVWriter.open(resultsFile)

    val conf = ConfigFactory.load("db")
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
      try {
        val input = CSVReader.open(csvInputFilename)
        new PatientSamples(input, output)
        input.close()
      } catch {
        case ex: FileNotFoundException => println(s"File does not exist: $csvInputFilename")
      }
    }

    output.close()
  }
}

/**
  * @param input Where the input is read from.
  *
  * @param output Where the output will be saved.
  *
  * @param countsOutput Where the counts information is saved to.
  */
class PatientSamples(input: CSVReader, output: CSVWriter)(implicit session: Session) {

  val log = Logger(LoggerFactory.getLogger(this.getClass))

  val dateFormat = DateTimeFormat.forPattern("yyyy-MM-dd")

  val ssPosInclude = List(
    "SSBH08",
    "SSBH09",
    "SSBH10",
    "SSBH11",
    "SSBH12",
    "SSBH13",
    "SSBH14",
    "SSBH15",
    "SSBJ01",
    "SSBJ02",
    "SSBJ03",
    "SSBJ04",
    "SSBJ05",
    "SSBJ06"
  )

  case class CollectionEvent(
    id: Int,
    visitNumber: Int,
    patientId: Int,
    activityStatusId: Int,
    version: Int
  )

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

  case class SpecimenDetails(
    id: Int,
    inventoryId: String,
    specimenTypeName: String,
    parentInventoryId: String,
    studyName: String,
    pnumber: String,
    visitNumber: Int,
    createdAt: DateTime,
    quantity: Option[BigDecimal],
    centreName: String,
    containerLabel: String,
    specimenPos: String,
    topContainerTypeName: String
  )

  implicit val getCollectioneventResult = GetResult(r =>
    CollectionEvent(r.<<, r.<<, r.<<, r.<<, r.<<))

  implicit val getSpecimenResult = GetResult(r =>
    Specimen(r.<<, r.<<, r.<<, r.<<, r.<<, r.<<, r.<<, r.<<, r.<<, r.<<, r.<<, r.<<, r.<<))

  implicit val getSpecimenDetailsResult = GetResult(r =>
    SpecimenDetails(r.<<, r.<<, r.<<, r.<<, r.<<, r.<<, r.<<, r.<<, r.<<, r.<<, r.<<, r.<<, r.<<))

  val VALID_SS_LABELS = """
    |'SSBH08', 'SSBH09', 'SSBH10', 'SSBH11', 'SSBH12', 'SSBH13', 'SSBH14', 'SSBH15',
    |'SSBJ01', 'SSBJ02', 'SSBJ03', 'SSBJ04', 'SSBJ05', 'SSBJ06'""".stripMargin

  val VALID_SPC_TYPES = """
    |'Serum B', 'SerumB100', 'SerumB300', 'SerumG100', 'SerumG300', 'SerumG400'
    """.stripMargin


  val SPECIMEN_QRY = s"""
    |SELECT spc.id, spc.inventory_id, stype.name_short, topspc.inventory_id, study.name_short, pt.pnumber,
    |   ce.visit_number, spc.created_at, spc.quantity, center.name_short, cntr.label,
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
    |WHERE pt.pnumber = ?
    |AND stype.name_short like 'Serum%'
    |AND topspc.id is not NULL
    |AND (DATEDIFF(topspc.created_at, ?) >= -5 AND DATEDIFF(topspc.created_at, ?) <= 5)
    |AND cntr.id is not NULL
    |AND (cntr.label not like 'SS%' OR cntr.label in ($VALID_SS_LABELS))
    |AND spc.activity_status_id = 1""".stripMargin

  // used to keep track of errors
  val errors = new ListBuffer[String]

  def getPatientInfo(pnumber: String, dateStr: String) = {
    val qry = Q.query[(String, String, String), SpecimenDetails](SPECIMEN_QRY)

    val specimens = qry((pnumber, dateStr, dateStr)).list

    if (specimens.isEmpty) {
      errors += s"no specimens found for patient: patient: $pnumber, date: $dateStr"
    } else {
      val specimen = specimens(0)
      log.debug(s"result: $specimen")

      val row = List(
        specimen.studyName,
        specimen.pnumber,
        specimen.visitNumber,
        specimens.size,
        specimen.inventoryId,
        specimen.specimenTypeName,
        dateFormat.print(specimen.createdAt),
        specimen.quantity.getOrElse("unknown"),
        specimen.centreName,
        specimen.containerLabel + specimen.specimenPos,
        specimen.topContainerTypeName
      )

      output.writeRow(row)
    }
  }

  output.writeRow(List(
      "study",
      "pnumber",
      "visitNumber",
      "specimensFromVisit",
      "inventoryId",
      "specimenType",
      "createdAt",
      "quantity",
      "centre",
      "label",
      "topContainerType"
    ))

  input.allWithHeaders.foreach{ csvRow =>
    //log.debug(s"$csvRow")
    getPatientInfo(csvRow("StudyNum"), csvRow("lbCollectionDate"))
  }

  // display errors in file
  if (!errors.isEmpty) {
    output.writeRow(List())
    errors.toList.foreach{ msg =>
      log.error(msg)
      output.writeRow(List(msg))
    }
  }

}
