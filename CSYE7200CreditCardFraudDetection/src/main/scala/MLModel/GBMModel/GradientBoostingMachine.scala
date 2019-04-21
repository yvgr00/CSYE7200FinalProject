package MLModel.GBMModel

import org.apache.log4j.{Level, Logger}
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.classification.GBTClassifier
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.feature.{StringIndexer, VectorAssembler}
import org.apache.spark.ml.tuning.{CrossValidator, ParamGridBuilder}
import org.apache.spark.mllib.evaluation.MulticlassMetrics
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Encoders, Row, SparkSession}

object GradientBoostingMachine {


  case class Credit(Time: Double, V1: Double, V2: Double, V3: Double, V4: Double, V5: Double, V6: Double, V7: Double, V8: Double, V9: Double, V10: Double, V11: Double, V12: Double, V13: Double, V14: Double, V15: Double, V16: Double, V17: Double, V18: Double, V19: Double, V20: Double, V21: Double, V22: Double, V23: Double, V24: Double, V25: Double, V26: Double, V27: Double, V28: Double, Amount: Double, Class: Double)


  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .master("local")
      .getOrCreate()

    import spark.implicits._

    val creditDS = spark
      .read
      .schema(Encoders.product[Credit].schema)
      .csv("src/main/resources/creditcard.csv")
      .as[Credit]

    creditDS.printSchema()

    creditDS.describe().show(false)

    val labelIndexer = new StringIndexer()
      .setInputCol("Class")
      .setOutputCol("iLabel")
      .setHandleInvalid("keep")
      .fit(creditDS)

    val featureCols = Array("Time","V1","V2","V3","V4","V5","V6","V7","V8","V9","V10","V11","V12","V13","V14","V15","V16","V17"
      ,"V18","V19","V20","V21","V22","V23","V24","V25","V26","V27","V28","Amount")

    val featureAssembler = new VectorAssembler()
      .setInputCols(featureCols)
      .setOutputCol("assembledFeatures")
      .setHandleInvalid("keep")



    val Array(trainingData, testData) = creditDS.randomSplit(Array(0.7, 0.3), seed = 5043)

    val gbm = new GBTClassifier()
      .setLabelCol("iLabel")
      .setFeaturesCol("assembledFeatures")
      .setMaxIter(15)
      .setFeatureSubsetStrategy("auto")


    val pipeline = new Pipeline()
      .setStages(Array(labelIndexer,featureAssembler, gbm))

    val model = pipeline.fit(trainingData)

    val predictions = model.transform(testData)

    predictions.show(false)

    predictions.select("iLabel", "probability","prediction").show(false)

    val evaluator = new MulticlassClassificationEvaluator()
      .setLabelCol("iLabel")
      .setPredictionCol("prediction")
      .setMetricName("accuracy")

    val accuracy = evaluator.evaluate(predictions)
    println(s"Test Error = ${(1.0 - accuracy)}")

    // Model Tuning

    val paramGrid = new ParamGridBuilder()
      .addGrid(gbm.maxBins, Array(40, 45, 50))
      .addGrid(gbm.maxDepth, Array(10, 12, 18))
      .build()

    val cv = new CrossValidator()
      .setEstimator(pipeline)
      .setEvaluator(evaluator)
      .setEstimatorParamMaps(paramGrid)
      .setParallelism(9)

    val tunedModel = cv.fit(trainingData)
    val tunedPredictions = tunedModel.transform(testData)
    tunedPredictions.select("Class", "probability","prediction").show(false)
    val tunedAccuracy = evaluator.evaluate(tunedPredictions)
    tunedPredictions.printSchema()
    val predict = tunedPredictions.select("iLabel","prediction")
    val hgt:RDD[Row] = predict.rdd
    val predictRdd = hgt.map(x => (x.getAs[Double](0),x.getAs[Double](1)))
    println(tunedAccuracy)
    println(s"Test Error = ${(1.0 - tunedAccuracy)}")

    Logger.getLogger("org.apache.spark").setLevel(Level.OFF)


    // Instantiate metrics object
    val metrics = new MulticlassMetrics(predictRdd)
    // Confusion matrix
    println("Confusion matrix:")
    println(metrics.confusionMatrix)

    // Precision by label
    val labels = metrics.labels
    labels.foreach { l =>
      println(s"Precision($l) = " + metrics.precision(l))
    }

    // Recall by label
    labels.foreach { l =>
      println(s"Recall($l) = " + metrics.recall(l))
    }

   // tunedModel.write.overwrite().save("src/main/scala/MLModel/GBModel")

  }


}
