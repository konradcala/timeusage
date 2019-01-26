package timeusage

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.dsl.expressions
import org.apache.spark.sql._
import org.apache.spark.sql.expressions.Aggregator
import org.apache.spark.{SparkConf, SparkContext}
import org.junit.runner.RunWith
import org.scalatest.{BeforeAndAfterAll, FunSuite, ShouldMatchers}
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class KonradSuite extends FunSuite with BeforeAndAfterAll with ShouldMatchers {
  val spark: SparkSession =
    SparkSession
      .builder()
      .appName("Time Usage")
      .config("spark.master", "local")
      .getOrCreate()

  import spark.implicits._

  ignore("group by") {
    import org.apache.spark.sql.expressions.scalalang.typed
    val tuples: List[(Int, Double, Int)] = List((1, 10.3, 2), (1, 20.55, 4), (2, 3, 8))
    val list = tuples.toDS()
    val aggregated: Dataset[(Int, Double, Double)] = list.groupByKey(_._1).agg(functions.round(typed.avg[(Int, Double, Int)](_._2), 1).as("dupa").as[Double], typed.avg(_._3))

    aggregated.show()

  }

  ignore("dataset") {
    val list = List((1, "konrad"), (2, "ala"), (1, "ola")).toDS()
    val byKey: KeyValueGroupedDataset[Int, (Int, String)] = list.groupByKey(pair => pair._1)
    println("pair")
    byKey.mapGroups((k, it) => (k, it.foldLeft("")((acc, p) => acc + p._2))).sort($"_1".desc).show
    byKey.mapValues(p => p._2).reduceGroups((acc, str) => acc + str).show()
    byKey.reduceGroups((a, b) => (a._1, a._2 + b._2)).show()
    val strContact = new Aggregator[(Int, String), String, String] {
      override def zero: String = ???

      override def reduce(b: String, a: (Int, String)): String = ???

      override def merge(b1: String, b2: String): String = ???

      override def finish(reduction: String): String = ???

      override def bufferEncoder: Encoder[String] = ???

      override def outputEncoder: Encoder[String] = ???
    }

    byKey.agg(strContact.toColumn)
    //    println("single")
    //    byKey.mapGroups((k, it) => it.foldLeft("")((acc, p) => acc + p._2)).show()
    //
    //    byKey.count().show()
    val frame: DataFrame = list.agg(functions.avg($"id").as[Double])
    //    frame.show()
  }

  ignore("toDebuString") {
    val largeList = (1 to 200).toList
    val wordRdd = spark.sparkContext.parallelize(largeList).map(w => (w, (w * w).toString))
    import spark.implicits._
    val frame: DataFrame = wordRdd.toDF("id", "name")
    val konrad: RDD[Row] = frame.rdd
    frame("id")
    frame.groupBy("id").count().show


    //    val pairs = wordRdd.map(c => (c, 1))
    //      .groupByKey()
    //    val debugString = pairs.toDebugString
    //
    //    val dep = pairs.dependencies
    //
    //    println(debugString)
    //    println(dep)

  }

}
