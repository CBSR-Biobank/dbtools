package org.biobank.dbtools

import org.joda.time.DateTime
import scala.slick.jdbc.GetResult
import com.github.tototoshi.slick.MySQLJodaSupport._

case class SpecimenDetails(
  id:                   Int,
  inventoryId:          String,
  specimenTypeName:     String,
  parentInventoryId:    String,
  pnumber:              String,
  visitNumber:          Int,
  dateDrawn:            DateTime,
  quantity:             Option[BigDecimal],
  centreName:           String,
  containerLabel:       String,
  specimenPos:          String,
  topContainerTypeName: String
)

object SpecimenDetails {

  implicit val specimenDetailsGetResult = GetResult(r =>
    SpecimenDetails(r.<<, r.<<, r.<<, r.<<, r.<<, r.<<, r.<<, r.<<, r.<<, r.<<, r.<<, r.<<))

}
