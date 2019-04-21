package KafkaSparkStreamingIntegration

import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.ml.tuning.CrossValidatorModel
import org.apache.spark.mllib.evaluation.MulticlassMetrics
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object StreamingML {


  def main(args:Array[String]){

    val conf = new SparkConf().setMaster("local[*]").setAppName("KafkaProject")
    val ssc = new StreamingContext(conf,Seconds(1))


    Logger.getLogger("org.apache.spark").setLevel(Level.OFF)

    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> "localhost:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "1",
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )
    val topics = List("test").toSet

      //[String,String,StringDecoder,StringDecoder]

    val lines = KafkaUtils.createDirectStream[String,String](ssc, LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](topics, kafkaParams))

    val data = lines.map(record => (record.key, record.value))

    val values = data.map(_._2)


    values.foreachRDD(rdd => {


      val spark = SparkSession.builder.config(rdd.sparkContext.getConf).getOrCreate()
      import spark.implicits._
      val rddtoDF = rdd.toDF
      val df = rddtoDF.selectExpr("split(value,',')[0] as Time"
        ,"split(value,',')[1] as V1"
        ,"split(value,',')[2] as V2"
        ,"split(value,',')[3] as V3"
        ,"split(value,',')[4] as V4"
        ,"split(value,',')[5] as V5"
        ,"split(value,',')[6] as V6"
        ,"split(value,',')[7] as V7"
        ,"split(value,',')[8] as V8"
        ,"split(value,',')[9] as V9"
        ,"split(value,',')[10] as V10"
        ,"split(value,',')[11] as V11"
        ,"split(value,',')[12] as V12"
        ,"split(value,',')[13] as V13"
        ,"split(value,',')[14] as V14"
        ,"split(value,',')[15] as V15"
        ,"split(value,',')[16] as V16"
        ,"split(value,',')[17] as V17"
        ,"split(value,',')[18] as V18"
        ,"split(value,',')[19] as V19"
        ,"split(value,',')[20] as V20"
        ,"split(value,',')[21] as V21"
        ,"split(value,',')[22] as V22"
        ,"split(value,',')[23] as V23"
        ,"split(value,',')[24] as V24"
        ,"split(value,',')[25] as V25"
        ,"split(value,',')[26] as V26"
        ,"split(value,',')[27] as V27"
        ,"split(value,',')[28] as V28"
        ,"split(value,',')[29] as Amount"
        ,"split(value,',')[30] as Class"
      )

      import org.apache.spark.sql.functions._
      val df2 = df.withColumn("Time", col("Time").cast("Double"))
        .withColumn("V1", col("V1").cast("Double"))
        .withColumn("V2", col("V2").cast("Double"))
        .withColumn("V3", col("V3").cast("Double"))
        .withColumn("V4", col("V4").cast("Double"))
        .withColumn("V5", col("V5").cast("Double"))
        .withColumn("V6", col("V6").cast("Double"))
        .withColumn("V7", col("V7").cast("Double"))
        .withColumn("V8", col("V8").cast("Double"))
        .withColumn("V9", col("V9").cast("Double"))
        .withColumn("V10", col("V10").cast("Double"))
        .withColumn("V11", col("V11").cast("Double"))
        .withColumn("V12", col("V12").cast("Double"))
        .withColumn("V13", col("V13").cast("Double"))
        .withColumn("V14", col("V14").cast("Double"))
        .withColumn("V15", col("V15").cast("Double"))
        .withColumn("V16", col("V16").cast("Double"))
        .withColumn("V17", col("V17").cast("Double"))
        .withColumn("V18", col("V18").cast("Double"))
        .withColumn("V19", col("V19").cast("Double"))
        .withColumn("V20", col("V20").cast("Double"))
        .withColumn("V21", col("V21").cast("Double"))
        .withColumn("V22", col("V22").cast("Double"))
        .withColumn("V23", col("V23").cast("Double"))
        .withColumn("V24", col("V24").cast("Double"))
        .withColumn("V25", col("V25").cast("Double"))
        .withColumn("V26", col("V26").cast("Double"))
        .withColumn("V27", col("V27").cast("Double"))
        .withColumn("V28", col("V28").cast("Double"))
        .withColumn("Amount", col("Amount").cast("Double"))
        .withColumn("Class", col("Class").cast("Double"))

      df2.show()

      val tempTable = df2.createOrReplaceTempView("creditcard")
      val wordCountsDataFrame = spark.sql("select * from creditcard")

     val model = CrossValidatorModel.load("src/main/scala/MLModel/RFModel")

     val predictions = model.transform(wordCountsDataFrame)

     val finalPredictions = predictions.select("Class","probability","prediction")

      finalPredictions.coalesce(1)
        .write.format("com.databricks.spark.csv")
        .option("header", "true")
        .save("src/main/resources/predictions.csv")

     finalPredictions.show()

      val predict = predictions.select("iLabel", "prediction")
      val hgt: RDD[Row] = predict.rdd
      val predictRdd = hgt.map(x => (x.getAs[Double](0), x.getAs[Double](1)))
      Logger.getLogger("org.apache.spark").setLevel(Level.OFF)
      val metrics = new MulticlassMetrics(predictRdd)

      println(metrics.confusionMatrix)

      println(metrics.recall(1))
      println(metrics.precision(1))
    })

    ssc.checkpoint("D:/Checkpoint/")
    ssc.start()
    ssc.awaitTermination()

  }

}
