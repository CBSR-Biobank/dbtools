package org.biobank.dbtools

import scala.slick.driver.MySQLDriver.simple._
import scala.slick.jdbc.{ GetResult, StaticQuery => Q }
import Q.interpolation
import org.joda.time.DateTime
import com.github.nscala_time.time.Imports._

object SpecimenDelete extends Command {

  val Name = "spcdelete"

  val Help =
    s"""|Deletes a single specimen from the database. """.stripMargin

  val Usage = s"$Name INVENTORY_ID_1 [INVENTORY_ID_2 .. INVENTORY_ID_n]"



  def invokeCommand(args: Array[String])(implicit session: Session): Unit = {
    if (args.length == 0) {
        println(s"\tError: inventory ID expected")
        println(s"\nusage: $Usage")
      System.exit(1)
    } else {
      args.foreach(invId => deleteSpecimen(invId))
    }
  }

  val DispatchQry =
    s"""|SELECT dspspc.dispatch_id
        |  FROM dispatch_specimen dspspc
        |  JOIN specimen spc ON spc.id=dspspc.specimen_id
        |  WHERE inventory_id=?""".stripMargin

  val DispatchInfoQry =
    s"""|SELECT dsp.id, COUNT(dspspc.id) AS cnt
        |  FROM DISPATCH dsp
        |  LEFT JOIN dispatch_specimen dspspc ON dspspc.dispatch_id=dsp.id
        |  WHERE dsp.id=?""".stripMargin

  val CeventQry =
    s"""|SELECT ce.id FROM collection_event ce
        |  LEFT JOIN specimen spc ON spc.collection_event_id=ce.id
        |  WHERE inventory_id=?""".stripMargin;

  val CeventInfoQry =
    s"""|SELECT ce.id, COUNT(spc.id) AS cnt
        |  FROM collection_event ce
        |  LEFT JOIN specimen spc ON spc.collection_event_id=ce.id
        |  WHERE ce.id=?""".stripMargin;

  case class DispatchInfo(id: Int, specimenCount: Int)

  case class CeventInfo(id: Int, specimenCount: Int)

  private def getDispatch(inventoryId: String)(implicit session: Session): Option[Int] = {
    val dispatchQry = Q.query[(String), Int](DispatchQry)
    dispatchQry((inventoryId)).firstOption
  }

  private def getDispatchInfo(id: Int)(implicit session: Session): DispatchInfo = {
    implicit val getDispatchInfoResult = GetResult(r => DispatchInfo(r.<<, r.<<))
    val dispatchQry = Q.query[(Int), DispatchInfo](DispatchInfoQry)
    dispatchQry((id)).first
  }

  private def getCevent(inventoryId: String)(implicit session: Session): Option[Int] = {
    val ceventQry = Q.query[(String), Int](CeventQry)
    ceventQry((inventoryId)).firstOption
  }

  private def getCeventInfo(id: Int)(implicit session: Session): CeventInfo = {
    implicit val getCeventInfoResult = GetResult(r => CeventInfo(r.<<, r.<<))
    val ceventQry = Q.query[(Int), CeventInfo](CeventInfoQry)
    ceventQry((id)).first
  }

  private def deleteDispatch(id: Int)(implicit session: Session) = {
    val dispatchInfo = getDispatchInfo(id)
    if (dispatchInfo.specimenCount == 0) {
      println(s"deleting dispatch $id")

      val rows = sqlu"DELETE dsp FROM dispatch dsp WHERE id = $id".first
      println(s"deleted $rows rows")
    }
  }

  /**  Deletes a Collection Event and also any Event Attributes associated with the collection event.
    */
  private def deleteCevent(id: Int)(implicit session: Session) = {
    val ceventInfo = getCeventInfo(id)

    if (ceventInfo.specimenCount == 0) {
      println(s"deleting collection event $id")

      var rows = sqlu"DELETE evattr FROM event_attr evattr WHERE collection_event_id = $id".first
      println(s"deleting event attributes for cevent: deleted $rows rows")

      rows = sqlu"DELETE ce FROM collection_event ce WHERE id = $id".first
      println(s"deleting collection event: deleted $rows rows")
    }
  }

  private def deleteSpecimen(inventoryId: String)(implicit session: Session) = {
    val specimenQry = Q.query[(String), Int]("""
       SELECT id from specimen where inventory_id = ?
    """)
    specimenQry((inventoryId)).firstOption.fold {
      println(s"specimen with inventory ID not found: $inventoryId")
    } { id =>
      println(s"deleting specimen $inventoryId")

      val dispatchId = getDispatch(inventoryId)
      val ceventId = getCevent(inventoryId)

      var rows = sqlu"UPDATE specimen spc SET spc.top_specimen_id = null WHERE spc.inventory_id = $inventoryId".first
      println(s"clearing out top_specimen: updated $rows rows")

      rows = sqlu"DELETE dspc FROM dispatch_specimen dspc JOIN specimen spc ON dspc.specimen_id=spc.id WHERE spc.inventory_id = $inventoryId".first
      println(s"deleting dipatch specimen: deleted $rows rows")

      rows = sqlu"DELETE spc FROM specimen spc WHERE inventory_id = $inventoryId".first
      println(s"deleting specimen: deleted $rows rows")

      dispatchId.map(id => deleteDispatch(id))
      ceventId.map(id => deleteCevent(id))
      ()
    }
  }

}
