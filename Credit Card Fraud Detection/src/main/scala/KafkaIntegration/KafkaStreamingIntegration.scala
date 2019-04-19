package KafkaIntegration

import kafka.serializer.StringDecoder
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.ml.tuning.CrossValidatorModel
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

object KafkaStreamingIntegration {

 // case class Credit(Time: Double, V1: Double, V2: Double, V3: Double, V4: Double, V5: Double, V6: Double, V7: Double, V8: Double, V9: Double, V10: Double, V11: Double, V12: Double, V13: Double, V14: Double, V15: Double, V16: Double, V17: Double, V18: Double, V19: Double, V20: Double, V21: Double, V22: Double, V23: Double, V24: Double, V25: Double, V26: Double, V27: Double, V28: Double, Amount: Double, Class: Double)


  def main(args: Array[String]): Unit = {
    // Create a spark session object
    val conf = new SparkConf().setMaster("local[*]").setAppName("KafkaProject")
    val ssc = new StreamingContext(conf,Seconds(1))


    Logger.getLogger("org.apache.spark").setLevel(Level.OFF)

    val kafkaParams = Map("bootstrap.servers" -> "localhost:9092")
    val topics = List("test").toSet

    val lines = KafkaUtils.createDirectStream[String,String,StringDecoder,StringDecoder](ssc,kafkaParams,topics).map(_._2)

    lines.foreachRDD(rdd => {


      val spark = SparkSession.builder.config(rdd.sparkContext.getConf).getOrCreate()
      import spark.implicits._
      val g = rdd.toDF
      val d = g.selectExpr("split(value,',')[0] as Time"
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
      val df2 = d.withColumn("Time", col("Time").cast("Double"))
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

      val b = df2.createOrReplaceTempView("words")
      val wordCountsDataFrame = spark.sql("select * from words")

      val model = CrossValidatorModel.load("D:/BIGDATASCALA/Assignments/CSYE7200/ScalaFinalProject/src/main/scala/SparkMLModel/RFModel")

      val predictions = model.transform(wordCountsDataFrame)

      val finalPredictions = predictions.select("Class","probability","prediction")

      finalPredictions.show()


      // wordCountsDataFrame.show()

    })



    /*  lines.foreachRDD { rdd =>

        // Get the singleton instance of SparkSession
        import org.apache.spark.sql.SparkSession
        val spark = SparkSession.builder.config(rdd.sparkContext.getConf).getOrCreate()

        // import spark.implicits._

        val wordsDataFrame = rdd.map(w => Credit(w(0),w(1),w(2),w(3),w(4),w(5),w(6),w(7),w(8),w(9),w(10),w(11),w(12),w(13),w(14),
          w(15),w(16),w(17),w(18),w(19),w(20),w(21),w(22),w(23),w(24),w(25),w(26),w(27),w(28),w(29),w(30))).toDF()

        // Convert RDD[String] to DataFrame

      }


    lines.foreachRDD(rdd => {

      println(rdd.toString())
      val spark = SparkSession.builder.config(rdd.sparkContext.getConf).getOrCreate()
      import spark.implicits._
      val g = rdd.map(x => x.split(","))
      g.map(j => j.map(d => println(d)))
      val t = g.map(x => x.map(j => j.toDouble))
      val j = t.map(w => Credit(w(0),w(1),w(2),w(3),w(4),w(5),w(6),w(7),w(8),w(9),w(10),w(11),w(12),w(13),w(14),w(15),w(16),w(17),w(18),w(19),w(20),w(21),w(22),w(23),w(24),w(25),w(26),w(27),w(28),w(29),w(30))).toDF
      j.createOrReplaceTempView("words")
      val wordCountsDataFrame = spark.sql("select * from words")
      wordCountsDataFrame.show()


    })







      //x.split(",")).map(x => (x(0),x(1),x(2),x(3),x(4),x(5),x(6))).map(x => Credit(x(0),x(1),x(2)))

      //val kht = lines.foreachRDD(x => x.map(x => x.length))


      //val result = lines.map(x => (x,1)).reduceByKeyAndWindow(_ + _ ,_ - _,Seconds(100),Seconds(1))
     // println(kht)
  */


    ssc.checkpoint("D:/Checkpoint/")
    ssc.start()
    ssc.awaitTermination()

  }


}
