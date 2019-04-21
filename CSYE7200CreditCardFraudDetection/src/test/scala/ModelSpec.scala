import MLModel.RandomForestAlgorithm.Credit
import org.apache.log4j.{Level, Logger}
import org.apache.spark.ml.tuning.CrossValidatorModel
import org.apache.spark.mllib.evaluation.MulticlassMetrics
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Encoders, Row, SparkSession}
import org.scalatest.{FlatSpec, Matchers}

class ModelSpec extends FlatSpec with Matchers {


  val spark = SparkSession
    .builder()
    .master("local[*]")
    .getOrCreate()

  import spark.implicits._

  val df = spark
    .read
    .schema(Encoders.product[Credit].schema)
    .csv("src/main/resources/testdata.csv/testData.csv")
    .as[Credit]

  df.createOrReplaceTempView("creditcard")
  val wordCountsDataFrame = spark.sql("select * from creditcard")
  val model = CrossValidatorModel.load("src/main/scala/MLModel/RFModel")
  val predictions = model.transform(wordCountsDataFrame)
  val predict = predictions.select("iLabel", "prediction")
  val hgt: RDD[Row] = predict.rdd
  val predictRdd = hgt.map(x => (x.getAs[Double](0), x.getAs[Double](1)))
  Logger.getLogger("org.apache.spark").setLevel(Level.OFF)
  val metrics = new MulticlassMetrics(predictRdd)



  "Precision of 0 " should "have" in {
    val prec0 = metrics.precision(0)
    prec0 should be > 0.8
  }

  "Recall of 1 " should "have" in {
    val rec1 = metrics.recall(1)
    rec1 should be > 0.8
  }

  "Recall of 0 " should "have" in {
    val rec0 = metrics.recall(0)
    rec0 should be > 0.8
  }

  "Precision of 1 " should "have" in {
    val prec1 = metrics.precision(1)
    prec1 should be > 0.77
  }


}