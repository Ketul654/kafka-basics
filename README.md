# Kafka Basics
## Kafka Cluster Setup

#### Follow below steps to set up 3 node cluster on single Mac machine

* Download Kafka from ```https://kafka.apache.org/downloads```
* Extract it somewhere by executing tar command on Terminal

  ```i.e. tar -xvf kafka_2.13-2.6.0.tgz```
* Go to that extracted Kafka folder
   
  ```i.e. cd kafka_2.13-2.6.0/```
* Start zookeeper
   
  ```bin/zookeeper-server-start.sh config/zookeeper.properties```
  
  This will bring up zookeeper on default port 2181 configured in ```config/zookeeper.properties``` file
* Start first broker/node

  ```bin/kafka-server-start.sh config/server.properties```  
  
  This will start broker with below default broker id, log directory and port configured in ```config/server.properties```
  ```   
  broker.id=0  
  log.dirs=/tmp/kafka-logs  
  port=9092
  ``` 
* Create a copy of ```config/server.properties``` file for second broker/node
   
  ```i.e. cp config/server.properties config/server1.properties```
* Change broker id, log directory and port in ```config/server1.properties``` file
   
  ```
  broker.id=1
  log.dirs=/tmp/kafka-logs-1
  port=9093
  ```
* Start second broker/node

  ```bin/kafka-server-start.sh config/server1.properties```    
* Create one more copy of ```config/server.properties``` file for third broker/node

  ```i.e. cp config/server.properties config/server2.properties```
* Change broker id, log directory and port in ```config/server2.properties``` file
   
  ```
  broker.id=2
  log.dirs=/tmp/kafka-logs-2
  port=9094
  ```   
* Start third broker/node

   ```bin/kafka-server-start.sh config/server2.properties```     
   
* Check what brokers are up and running

  ```bin/zookeeper-shell.sh localhost:2181 ls /brokers/ids```  
  
  will give you below output
  
  ```
  Connecting to localhost:2181
 
  WATCHER::
  
  WatchedEvent state:SyncConnected type:None path:null
  [0, 1, 2]
  ```

## Kafka Topic 

* Create topic

  ```bin/kafka-topics.sh --create --bootstrap-server localhost:9092 --replication-factor 3 --partitions 1 --topic first-topic```
* Check if topic is created 

  ```bin/kafka-topics.sh --list --zookeeper localhost:2181```     
## Kafka Console Producer

* Start console producer and produce messages

  ```
  [ kafka_2.13-2.6.0 % bin/kafka-console-producer.sh --broker-list localhost:9092 --topic first-topic
  >Hi
  >How are you ?
  >This is my first message
  >
  ```
  
  You can also produce message with key using below command
  
  ```$xslt
  [ kafka_2.13-2.6.0 % bin/kafka-console-producer.sh --broker-list localhost:9092 --topic first-topic --property parse.key=true --property key.separator=":"
  >name:ketul
  >localtion:glasgow
  ```
## Kafka Console Consumer

* Start console consumer to consume messages produced by producer
  
  ```
  [ kafka_2.13-2.6.0 % bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic first-topic --property print.key=true --property print.value=true --from-beginning 
  null	Hi
  null	How are you ?
  null	This is my first message
  ```
