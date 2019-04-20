# CSYE7200FinalProject
# CREDIT CARD FRAUD DETECTION USING APACHE KAFKA AND SPARK STREAMING - SCALA 

In order to run our project, we need to have **Apache kafka and Spark** installed 


<a href = "https://kafka.apache.org/quickstart"> Apache Kafka Guide </a>

<a href = "https://spark.apache.org/downloads.html"> Spark Guide </a>

Now install git:
```
sudo yum install -y git
sudo apt-get install git
```
Clone the repository:
```
git clone https://github.com/yvgr00/CSYE7200FinalProject.git
```
For your kafka setup --> open three terminal to run
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
Now when you run the source code in the repository, your spark streaming will be started and subscribe to kafka topic.
```
val conf = new SparkConf().setMaster("local[*]").setAppName("KafkaProject")
val ssc = new StreamingContext(conf,Seconds(1))
ssc.start()
ssc.awaitTermination()
```

In this project, we used Random forest classifier model to predict whether its a fraudulent transaction or not.


<a href =  "https://github.com/yvgr00/CSYE7200FinalProject/blob/master/Credit%20Card%20Fraud%20Detection/src/main/scala/SparkMLModel/RandomForestAlgorithm.scala"> Random Forest Classifier </a>
