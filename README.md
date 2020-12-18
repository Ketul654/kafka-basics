# Kafka Basics
## Kafka Cluster Setup

#### Follow below steps to set up 3 node cluster on single Mac machine

* Download Kafka from ```https://kafka.apache.org/downloads```
* Extract it somewhere
* Go to that extracted Kafka folder on Terminal
   
  ```i.e. cd kafka_2.13-2.6.0/```
* Start zookeeper
   
  ```bin/zookeeper-server-start.sh config/zookeeper.properties```
  
  This will bring zookeeper on default port 2181 configured in ```config/zookeeper.properties``` file
* Start first broker/node

  ```bin/kafka-server-start.sh config/server.properties```  
  
  This will start broker with below default broker id, log directory~~~~~~~~~~~~~~~~~~~~ and port configured in ```config/server.properties```
     
  ```broker.id=0```
     
  ```log.dirs=/tmp/kafka-logs```
     
  ```port=9092``` 
* Create a copy of ```config/server.properties``` file for second broker/node
   
  ```i.e. cp config/server.properties config/server1.properties```
* Change broker id, log directory and port in ``config/server1.properties``` file
   
   ```broker.id=1```
   
   ```log.dirs=/tmp/kafka-logs-1```
   
   ```port=9093```
* Start second broker/node

  ```bin/kafka-server-start.sh config/server1.properties```    
* Create one more copy of ```config/server.properties``` file for third broker/node

  ```i.e. cp config/server.properties config/server2.properties```
* Change broker id, log directory and port in ```config/server2.properties``` file
   
   ```broker.id=2```
   
   ```log.dirs=/tmp/kafka-logs-2```
   
   ```port=9094```   
* Start third broker/node

   ```bin/kafka-server-start.sh config/server2.properties```     
   
   