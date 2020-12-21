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

  ```
  bin/kafka-topics.sh --create --bootstrap-server localhost:9092 \
  --replication-factor 3 \
  --partitions 1 \
  --topic single-partition-topic
  ```
  
  ```
  bin/kafka-topics.sh --create --bootstrap-server localhost:9092 \
  --replication-factor 3 \
  --partitions 3 \
  --topic multi-partition-topic
  ```
* Check if topics are created 

  ```$xslt
  [ kafka_2.13-2.6.0 % bin/kafka-topics.sh --list --zookeeper localhost:2181                                                                      
  __consumer_offsets
  single-partition-topic
  multi-partition-topic
  ```  
  
* Check topic description
  ```$xslt
  [ kafka_2.13-2.6.0 % bin/kafka-topics.sh --describe --topic single-partition-topic --zookeeper localhost:2181
  Topic: single-partition-topic	PartitionCount: 1	ReplicationFactor: 3	Configs: 
  	Topic: single-partition-topic	Partition: 0	Leader: 1	Replicas: 1,2,0	Isr: 1,2,0
  ```    

* You can clear kafka topic by updating retention config

  ```$xslt
  [ kafka_2.13-2.6.0 % bin/kafka-configs.sh --bootstrap-server localhost:9092 \
  --alter \
  --entity-type topics \
  --entity-name multi-partition-topic \
  --add-config retention.ms=1
  Completed updating config for topic multi-partition-topic.
  ```  
  
  You can check if config is updated
  
  ```$xslt
  [ kafka_2.13-2.6.0 % bin/kafka-configs.sh --bootstrap-server localhost:9092 \
  --describe \
  --entity-type topics \
  --entity-name multi-partition-topic
  Dynamic configs for topic multi-partition-topic are:
    retention.ms=1000 sensitive=false synonyms={DYNAMIC_TOPIC_CONFIG:retention.ms=1000}
  ```
  
  Make sure you remove this retention config or reset it to previous value once topic is purged otherwise it will keep purging.
  
  ```$xslt
  [ kafka_2.13-2.6.0 % bin/kafka-configs.sh --bootstrap-server localhost:9092 \
  --alter \
  --entity-type topics \
  --entity-name multi-partition-topic \
  --delete-config retention.ms                           
  Completed updating config for topic multi-partition-topic.
  ```
## Kafka Console Producer

* Start console producer and produce messages

  ```
  [ kafka_2.13-2.6.0 % bin/kafka-console-producer.sh --broker-list localhost:9092 --topic single-partition-topic
  >Hi
  >How are you ?
  >This is my first message
  >
  ```
  
* You can also produce message with key using below command
  
  ```$xslt
  [ kafka_2.13-2.6.0 % bin/kafka-console-producer.sh --broker-list localhost:9092 \
  --topic single-partition-topic \
  --property parse.key=true \
  --property key.separator=":"
  >name:ketul
  >localtion:glasgow
  ```
## Kafka Console Consumer

* Start console consumer to consume messages produced by producer
  
  ```
  [ kafka_2.13-2.6.0 % bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 \
  --topic single-partition-topic \
  --property print.key=true \
  --property print.value=true \
  --from-beginning 
  null	Hi
  null	How are you ?
  null	This is my first message
  ```
* You can also consume from a specific partition of a topic
  
  ```$xslt
  [ kafka_2.13-2.6.0 % bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 \
  --topic multi-partition-topic \
  --property print.key=true \
  --property print.value=true \
  --from-beginning \
  --partition 1
  0	Message 0 to direct partition 1:e557cef7-7086-4b3f-a0e7-7b9e9e408cc2
  1	Message 1 to direct partition 1:17b0b376-cc6c-4b32-b3c3-40ab987da240
  2	Message 2 to direct partition 1:0be00510-abfc-4474-a379-db3685407ea5
  3	Message 3 to direct partition 1:68a16e86-5985-4019-8619-ce4fad7f32dc
  4	Message 4 to direct partition 1:431dcc32-ba86-49b8-9386-a81b9f2e206e
  5	Message 5 to direct partition 1:2f108526-327c-4fcf-b4ba-8d45dc9fab71
  ```