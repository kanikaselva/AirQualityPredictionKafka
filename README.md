# Real-Time Air Quality Prediction with Apache Kafka

Developed an end-to-end data pipeline using the UCI Air Quality dataset to demonstrate proficiency in streaming architecture, exploratory data analysis, and predictive modeling deployment. The analysis of hourly air quality measurements is from an Italian city over a one-year period from March 2004 to February 2005. The technical architecture for this project was designed to simulate a real-time streaming environment using the UCI Air Quality dataset through a single-node Kafka cluster implemented in KRaft mode. After collecting data, visualization and advanced analytics such as cyclic trends analysis and decomposition analysis was performed to understand the data. Finally, predictive analysis was performed using baseline measurement (ffill), random forest and LSTM.

**To run the files, one must run kafka first. If the user does not have a local binary file, follow the steps below:**

```
cd ~
```

```
rm -rf kafka_2.13-4.1.0
```

```
curl -O https://downloads.apache.org/kafka/4.1.0/kafka_2.13-4.1.0.tgz
```

```
tar -xvzf kafka_2.13-4.1.0.tgz
```

```
cd kafka_2.13-4.1.0
```

```
./bin/kafka-storage.sh random-uuid
```

```
UUID=$(./bin/kafka-storage.sh random-uuid)
```

```
echo $UUID
```

```
./bin/kafka-storage.sh format -t $UUID -c ./config/kraft/server.properties
```

```
./bin/kafka-server-start.sh ./config/kraft/server.properties
```

**Open a new terminal to create a topic**

```
./bin/kafka-topics.sh --create --topic air_quality --bootstrap-server localhost:9092
```

If you run through the following error:

```
java.nio.file.NoSuchFileException: ./config/kraft/server.properties
```

Create your own server.properties file by following these steps below:

```
mkdir -p config/kraft
```

```
nano config/kraft/server.properties
```

===== KRaft Mode single node =====

process.roles=broker,controller

node.id=1

controller.quorum.voters=1@localhost:9093

Listeners

listeners=PLAINTEXT://:9092,CONTROLLER://:9093

Define security protocols

listener.security.protocol.map=PLAINTEXT:PLAINTEXT,CONTROLLER:PLAINTEXT

Log directories

log.dirs=/tmp/kraft-logs

Topics and replication

num.partitions=1

offsets.topic.replication.factor=1

transaction.state.log.replication.factor=1

transaction.state.log.min.isr=1

**After saving, run again:**

```
./bin/kafka-storage.sh format -t $UUID -c ./config/kraft/server.properties
```

```
./bin/kafka-server-start.sh ./config/kraft/server.properties
```

**To run the .py and .ipynb files, make sure the following modules are installed in your python environment:**

* pandas
* numpy
* scikit-learn
* kafka
* json
* logging
* datetime
* os
* ucimlrepo
* matplotlib
* seaborn
* statsmodels
* tenserflow
