package KafkaIntegration

import org.apache.log4j.{Level, Logger}
import org.apache.spark.ml.tuning.CrossValidatorModel
import org.apache.spark.sql.types.{DoubleType, StructField, StructType}
import org.apache.spark.sql.{Encoders, SparkSession}

object newthing {


  case class Credit(Time: Double, V1: Double, V2: Double, V3: Double, V4: Double, V5: Double, V6: Double, V7: Double, V8: Double, V9: Double, V10: Double, V11: Double, V12: Double, V13: Double, V14: Double, V15: Double, V16: Double, V17: Double, V18: Double, V19: Double, V20: Double, V21: Double, V22: Double, V23: Double, V24: Double, V25: Double, V26: Double, V27: Double, V28: Double, Amount: Double, Class: Double)

  def main(args: Array[String]): Unit = {
    // Create a spark session object
    val ss = SparkSession.builder()
      .appName("Spark kafka Credit Card Fraud Detection")
      .master("local[*]")
      .getOrCreate()
    // Define schema of the topic to be consumed

    Logger.getLogger("org.apache.spark").setLevel(Level.OFF)


    val schema = StructType(
      Array(StructField("Time", DoubleType, true),
        StructField("V1", DoubleType, true),
        StructField("V2", DoubleType, true),
        StructField("V3", DoubleType, true),
        StructField("V4", DoubleType, true),
        StructField("V5", DoubleType, true),
        StructField("V6", DoubleType, true),
        StructField("V7", DoubleType, true),
        StructField("V8", DoubleType, true),
        StructField("V9", DoubleType, true),
        StructField("V10", DoubleType, true),
        StructField("V11", DoubleType, true),
        StructField("V12", DoubleType, true),
        StructField("V13", DoubleType, true),
        StructField("V14", DoubleType, true),
        StructField("V15", DoubleType, true),
        StructField("V16", DoubleType, true),
        StructField("V17", DoubleType, true),
        StructField("V18", DoubleType, true),
        StructField("V19", DoubleType, true),
        StructField("V20", DoubleType, true),
        StructField("V21", DoubleType, true),
        StructField("V22", DoubleType, true),
        StructField("V23", DoubleType, true),
        StructField("V24", DoubleType, true),
        StructField("V25", DoubleType, true),
        StructField("V26", DoubleType, true),
        StructField("V27", DoubleType, true),
        StructField("V28", DoubleType, true),
        StructField("Amount", DoubleType, true),
        StructField("Class", DoubleType, true)
      ))


    /* val streamingDataFrame = ss.readStream.schema(schema).csv("D:\\data\\")

    streamingDataFrame.selectExpr("CAST(Time AS STRING) AS key", "to_json(struct(*)) AS value").
       writeStream
       .format("kafka")
       .option("topic", "test")
       .option("kafka.bootstrap.servers", "localhost:9092")
       .option("checkpointLocation", "D:\\streamingoffsets")
       .start()*/

    import ss.implicits._
    val df = ss
      .readStream
      .schema(Encoders.product[Credit].schema)
      .csv("D:\\data\\")
      .as[Credit]


    //val df2 = df.as[Credit]
   /* val interval2= df
      .selectExpr("split(value,',')[0] as Time"
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

    /* val df2 = interval2.select(Seq("Time","V1","V2","V3","V4","V5","V6","V7","V8","V9","V10","V11","V12","V13","V14","V15","V16","V17"
       ,"V18","V19","V20","V21","V22","V23","V24","V25","V26","V27","V28","Amount","Class").map(
       c => col(c).cast("double")
     ): _*) */

    import org.apache.spark.sql.functions._
    val df2 = interval2.withColumn("Time", col("Time").cast("Double"))
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
      .withColumn("Class", col("Class").cast("Double")) */





    val model = CrossValidatorModel.load("D:/BIGDATASCALA/Assignments/CSYE7200/Credit Card Fraud Detection/src/main/scala/SparkMLModel/RFModel")

    val predictions = model.transform(df)

    val finalPredictions = predictions.select("Class","probability","prediction")

    finalPredictions.printSchema()





    val query = finalPredictions
      .writeStream.outputMode("append")
      .format("console").start()
      //.option("path", "D:\\Results\\") .option("checkpointLocation", "D:\\data\\").start()


    //val consoleOutput = df.writeStream.outputMode("append").format("console").start()


    query.awaitTermination()


  }

}
