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
  */
object PatientSamples {

  case class DbSettings(host: String, name: String, user: String, password: String)

  val specimensFilename = "specimens.csv"
  val spcTypeCountFilename = "spcTypeCounts.csv"

  def main(args: Array[String]) = {
    if (args.size < 1) {
      println("\tError: no CSV files specified")
      System.exit(1)
    } else if (args.size > 1) {
      println("\ttoo many paramters")
      System.exit(1)
    }

    val csvInputFilename = args(0)
    val specimensCsvWriter = CSVWriter.open(specimensFilename)
    val spcTypeCountCsvWriter = CSVWriter.open(spcTypeCountFilename)

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
        val csvReader = CSVReader.open(csvInputFilename)
        new PatientSamples(csvReader, specimensCsvWriter, spcTypeCountCsvWriter)
        csvReader.close()
      } catch {
        case ex: FileNotFoundException => println(s"File does not exist: $csvInputFilename")
      }
    }

    specimensCsvWriter.close()
    spcTypeCountCsvWriter.close()
  }
}

/**
  * @param csvReader Where the csvReader is read from.
  *
  * @param specimensCsvWriter Where the specimensCsvWriter will be saved.
  */
class PatientSamples(
  csvReader: CSVReader,
  specimensCsvWriter: CSVWriter,
  spcTypeCountCsvWriter: CSVWriter)(implicit session: Session) {

  val log = Logger(LoggerFactory.getLogger(this.getClass))

  val dateFormat = DateTimeFormat.forPattern("yyyy-MM-dd")

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

  implicit val getCollectioneventResult = GetResult(r =>
    CollectionEvent(r.<<, r.<<, r.<<, r.<<, r.<<))

  implicit val getSpecimenResult = GetResult(r =>
    Specimen(r.<<, r.<<, r.<<, r.<<, r.<<, r.<<, r.<<, r.<<, r.<<, r.<<, r.<<, r.<<, r.<<))

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

  val VALID_SS_LABELS = """
    |'SSBH08', 'SSBH09', 'SSBH10', 'SSBH11', 'SSBH12', 'SSBH13', 'SSBH14', 'SSBH15',
    |'SSBJ01', 'SSBJ02', 'SSBJ03', 'SSBJ04', 'SSBJ05', 'SSBJ06'""".stripMargin

  val VALID_SPC_TYPES = """
    |'Serum B', 'SerumB100', 'SerumB300', 'SerumG100', 'SerumG300', 'SerumG400'
    """.stripMargin

  val BASE_QRY = s"""
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
    |AND stype.name_short in ($VALID_SPC_TYPES)
    |AND topspc.id is not NULL
    |AND (DATEDIFF(topspc.created_at, ?) >= -5 AND DATEDIFF(topspc.created_at, ?) <= 5)
    |AND cntr.id is not NULL
    |AND (cntr.label not like 'SS%' OR cntr.label in ($VALID_SS_LABELS))
    |AND spc.activity_status_id = 1""".stripMargin


  val SPECIMEN_QRY = s"""
    |SELECT spc.id, spc.inventory_id, stype.name_short, topspc.inventory_id, pt.pnumber,
    |   ce.visit_number, topspc.created_at, spc.quantity, center.name_short, cntr.label,
    |   spos.position_string, top_cntr_type.name_short
    |$BASE_QRY""".stripMargin

  val SPECIMEN_TYPE_COUNT_QRY = s"""
    |SELECT pt.pnumber, ce.visit_number, topspc.created_at, stype.name_short, count(*)
    |$BASE_QRY
    |GROUP BY stype.name_short""".stripMargin

  // used to keep track of errors
  val errors = new ListBuffer[String]

  def getSpecimenInfo(pnumber: String, dateStr: String) = {
    case class SpecimenDetails(
      id: Int,
      inventoryId: String,
      specimenTypeName: String,
      parentInventoryId: String,
      pnumber: String,
      visitNumber: Int,
      dateDrawn: DateTime,
      quantity: Option[BigDecimal],
      centreName: String,
      containerLabel: String,
      specimenPos: String,
      topContainerTypeName: String
    )

    implicit val getSpecimenDetailsResult = GetResult(r =>
      SpecimenDetails(r.<<, r.<<, r.<<, r.<<, r.<<, r.<<, r.<<, r.<<, r.<<, r.<<, r.<<, r.<<))

    val qry = Q.query[(String, String, String), SpecimenDetails](SPECIMEN_QRY)

    val specimens = qry((pnumber, dateStr, dateStr)).list

    if (specimens.isEmpty) {
      errors += s"no specimens found for patient: patient: $pnumber, date: $dateStr"
    } else {
      specimens.foreach{ specimen =>
        log.debug(s"result: $specimen")

        val row = List(
          specimen.pnumber,
          specimen.visitNumber,
          dateFormat.print(specimen.dateDrawn),
          specimen.inventoryId,
          specimen.specimenTypeName,
          specimen.centreName,
          specimen.containerLabel + specimen.specimenPos,
          specimen.topContainerTypeName
        )

        specimensCsvWriter.writeRow(row)
      }

      specimensCsvWriter.writeRow(List())
    }
  }

  def getSpecimenCountInfo(pnumber: String, dateStr: String) = {
    case class SpecimenTypeCount(
      pnumber: String,
      visitNumber: Int,
      dateDrawn: DateTime,
      specimenTypeName: String,
      count: Int)

    implicit val getSpecimenTypeCountResult = GetResult(r =>
      SpecimenTypeCount(r.<<, r.<<, r.<<, r.<<, r.<<))

    val qry = Q.query[(String, String, String), SpecimenTypeCount](SPECIMEN_TYPE_COUNT_QRY)

    val specimenCounts = qry((pnumber, dateStr, dateStr)).list

    specimenCounts.foreach{ specimenCount =>
      log.debug(s"result: $specimenCount")

      val row = List(
        specimenCount.pnumber,
        specimenCount.visitNumber,
        dateFormat.print(specimenCount.dateDrawn),
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

  csvReader.allWithHeaders.foreach{ csvRow =>
    //log.debug(s"$csvRow")
    getSpecimenInfo(csvRow("StudyNum"), csvRow("lbCollectionDate"))
    getSpecimenCountInfo(csvRow("StudyNum"), csvRow("lbCollectionDate"))
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
