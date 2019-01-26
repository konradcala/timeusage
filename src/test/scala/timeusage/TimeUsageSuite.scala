package timeusage

import org.apache.spark.sql.{ColumnName, DataFrame, Row}
import org.apache.spark.sql.types.{DoubleType, StringType, StructField, StructType}
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.{BeforeAndAfterAll, FunSuite, Matchers}
import timeusage.TimeUsage._
import org.apache.spark.sql.functions.col

import scala.util.Random

@RunWith(classOf[JUnitRunner])
class TimeUsageSuite extends FunSuite with BeforeAndAfterAll with Matchers {

  test("dfSchema") {
    val struct: StructType = TimeUsage.dfSchema(List("ola", "ala", "ela"))
    struct("ola").dataType should be(StringType)
    struct("ala").dataType should be(DoubleType)
    struct("ela").dataType should be(DoubleType)
    struct.fields.foreach(field => field.nullable should be(false))
  }

  test("row") {
    val row: Row = TimeUsage.row(List("ola", "1", "-33"))
    row.size should be(3)
  }

  ignore("read") {
    val (columns, initDf) = read("/timeusage/atussum.csv")
    val (primaryNeedsColumns, workColumns, otherColumns) = classifiedColumns(columns)
    val summaryDf = timeUsageSummary(primaryNeedsColumns, workColumns, otherColumns, initDf)
    val dataset = timeUsageSummaryTyped(summaryDf)
    val first = dataset.collect().apply(0)
    first should be(TimeUsageRow("working", "male", "elder", 15.25, 0.0, 8.75))

    //    val groupedDf = TimeUsage.timeUsageGrouped(summaryDf)
    //    groupedDf.show()

    //    summaryDf.show(10)
    //    initDf.show(10)
  }

  test("classifiedColumns") {
    TimeUsage.classifiedColumns(List("t0123", "t02123", "t05123")) should be(List(col("t0123")), List(col("t05123")), List(col("t02123")))
    val in = List("t180101")
    TimeUsage.classifiedColumns(in) should be (List(col("t180101")), List(), List())
  }
}
