# CREDIT CARD FRAUD DETECTION USING APACHE KAFKA AND SPARK STREAMING - CSYE 7200

```
Authors:
Nancy Jemimah Packiyanathan
Venu Gopal Reddy Yerragunta
```

**Caution : Please be aware of the version compatability so that you won't be getting an error**

In order to run our project, we need to have **Apache kafka and Spark** installed 


<a href = "https://kafka.apache.org/quickstart"> Apache Kafka Guide </a>

<a href = "https://spark.apache.org/downloads.html"> Spark Guide </a>

**Now install git:**
```
sudo yum install -y git
sudo apt-get install git
```
**Clone the repository:**
```
git clone https://github.com/yvgr00/CSYE7200FinalProject.git
```

As the volume of the data is so huge, we recommend to download the data from the link and place it in the resource folder inside src


**Data Source** : <a href = "https://www.kaggle.com/mlg-ulb/creditcardfraud"> Credit Card Dataset </a> 

**For your kafka setup --> open three terminal to run**
```
1.First start a ZooKeeper server if you don't already have one
zookeeper-server-start.bat .../.../config/zookeeper.properties
2.Now start the Kafka server
kafka-server-start.bat .../.../config/server.properties
3.Create a kafka topic
kafka-topics.sh --create --bootstrap-server localhost:9092 --replication-factor 1 --partitions 1 --topic test
See the topic by running the below command
kafka-topics.bat --list --bootstrap-server localhost:9092
```
Now when you run the source code in the repository, your **spark streaming** will be started and subscribe to kafka topic.

<a href = "https://github.com/yvgr00/CSYE7200FinalProject/blob/master/Credit%20Card%20Fraud%20Detection/src/main/scala/KafkaIntegration/KafkaStreamingIntegration.scala"> Kafka-Spark Streaming </a>
```
val conf = new SparkConf().setMaster("local[*]").setAppName("KafkaProject")
val ssc = new StreamingContext(conf,Seconds(1))
ssc.start()
ssc.awaitTermination()
```

In this project, we used Random forest classifier model and Gradient Boosting Tree model to detect the fraudulent transactions
Compared the result and we used the Random Forest Classifier model in our Spark Streaming data.

**Random Forest**

<a href =  "https://github.com/yvgr00/CSYE7200FinalProject/blob/master/Credit%20Card%20Fraud%20Detection/src/main/scala/SparkMLModel/RandomForestAlgorithm.scala"> Random Forest Classifier </a>

**Gradient Boosted Tree**

<a href = "https://github.com/yvgr00/CSYE7200FinalProject/blob/master/CSYE7200CreditCardFraudDetection/src/main/scala/MLModel/GBMModel/GradientBoostingMachine.scala"> Gradient Boosting Tree </a>



**Achieved Acceptance Criteria:**
``` scala
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

Precision of Class = 0 --> 0.99
Precision of Class = 1 --> 0.78
Recall of Class = 0 --> 0.99
Recall of Class =1 --> 0.91
```
**Created unit test cases for checking precision and recall**

Thank you :)
