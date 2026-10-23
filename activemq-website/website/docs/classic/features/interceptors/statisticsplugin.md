---
title: "StatisticsPlugin"
slug: /statisticsplugin
---

Beginning in ActiveMQ 5.3, a statistics plugin is included that can be used to retrieve statistics from the broker or its destinations. Note that the message must contain a `replyTo` header (the `jmsReplyTo` header if you're using JMS) else the message will be ignored. The `replyTo` header must contain the name of the destination from which you want to retrieve the stats message(s). The statistics message is a `MapMessage` populated with statistics for the target (i.e., a broker or a destination).

To retrieve stats for the broker, just send an empty message to the destination named `ActiveMQ.Statistics.Broker` along with a `replyTo` header. To retrieve stats for a destination, just send an empty message to the destination named `ActiveMQ.Statistics.Destination.<destination-name>` or `ActiveMQ.Statistics.Destination.<wildcard-expression>` along with a `replyTo` header. If many destinations match a given wildcard expression, one stats message for each destination will be sent to the `replyTo` destination.

To configure ActiveMQ to use the statistics plugin, just add the following to the ActiveMQ XML configuration:
```
<broker ...>
  <plugins>
    <statisticsBrokerPlugin/>
  </plugins>
</broker>
```
The statistics plugin looks for messages sent to particular destinations. Below is an example of using the statistics plugin to grab stats from a broker:
```
Queue replyTo = session.createTemporaryQueue();
MessageConsumer consumer = session.createConsumer(replyTo);

String queueName = "ActiveMQ.Statistics.Broker";
Queue testQueue = session.createQueue(queueName);
MessageProducer producer = session.createProducer(testQueue);
Message msg = session.createMessage();
msg.setJMSReplyTo(replyTo);
producer.send(msg);

MapMessage reply = (MapMessage) consumer.receive();
assertNotNull(reply);

for (Enumeration e = reply.getMapNames();e.hasMoreElements();) {
  String name = e.nextElement().toString();
  System.out.println(name + "=" + reply.getObject(name));
}
```
The output from the code above is shown below:
```
vm=vm://localhost  
memoryUsage=0  
storeUsage=3330  
tempPercentUsage=0  
ssl=  
openwire=tcp://localhost:50059  
brokerId=ID:bigmac-50057-1253605065511-0:0  
consumerCount=2  
brokerName=localhost  
expiredCount=0  
dispatchCount=1  
maxEnqueueTime=5.0  
storePercentUsage=0  
dequeueCount=0  
inflightCount=1  
messagesCached=0  
tempLimit=107374182400  
averageEnqueueTime=5.0  
stomp+ssl=  
memoryPercentUsage=0  
size=10  
tempUsage=0  
producerCount=1  
minEnqueueTime=5.0  
dataDirectory=/Users/rajdavies/dev/projects/activemq/activemq-core/activemq-data  
enqueueCount=10  
stomp=  
storeLimit=107374182400  
memoryLimit=67108864
```
Similarly, to query the statistics for a destination just send a message to the destination name prepended with `ActiveMQ.Statistics.Destination`. For example, to retrieve the statistics for a queue whose name is TEST.FOO, send an empty message to the queue named `ActiveMQ.Statistics.Destination.TEST.FOO`. Below is an example:
```
Queue replyTo = session.createTemporaryQueue();
MessageConsumer consumer = session.createConsumer(replyTo);

Queue testQueue = session.createQueue("TEST.FOO");
MessageProducer producer = session.createProducer(null);

String queueName = "ActiveMQ.Statistics.Destination." + testQueue.getQueueName()
Queue query = session.createQueue(queueName);

Message msg = session.createMessage();

producer.send(testQueue, msg) 
msg.setJMSReplyTo(replyTo);
producer.send(query, msg);
MapMessage reply = (MapMessage) consumer.receive();
assertNotNull(reply);
assertTrue(reply.getMapNames().hasMoreElements());
        
for (Enumeration e = reply.getMapNames();e.hasMoreElements();) {
    String name = e.nextElement().toString();
    System.err.println(name + "=" + reply.getObject(name));
}
```
The output from the code above is shown below:
```
memoryUsage=0  
dequeueCount=0  
inflightCount=0  
messagesCached=0  
averageEnqueueTime=0.0  
destinationName=queue://TEST.FOO  
size=1  
memoryPercentUsage=0  
producerCount=0  
consumerCount=0  
minEnqueueTime=0.0  
maxEnqueueTime=0.0  
dispatchCount=0  
expiredCount=0  
enqueueCount=1  
memoryLimit=67108864
```
You can also use wildcards in the queue name, too. This will result in a separate stats message for every destination that is matched by the wildcard. Very handy indeed.

### Subscriptions statistics

Since 5.6.0 you can also retrieve statistics on all queue and topic subscriptions. All you need to do it send an empty message to the destination named `ActiveMQ.Statistics. Subscription` along with a `replyTo` header.  The response will come in the form of one or more messages each containing the statistics for exactly one subscription on the Broker. 

Below is an example of using the statistics plugin to grab stats from a broker:
```
Queue replyTo = session.createTemporaryQueue();
MessageConsumer consumer = session.createConsumer(replyTo);

String queueName = "ActiveMQ.Statistics.Subscription";
Queue testQueue = session.createQueue(queueName);
MessageProducer producer = session.createProducer(testQueue);
Message msg = session.createMessage();
msg.setJMSReplyTo(replyTo);
producer.send(msg);

MapMessage reply = (MapMessage) consumer.receive();
assertNotNull(reply);

for (Enumeration e = reply.getMapNames();e.hasMoreElements();) {
  String name = e.nextElement().toString();
  System.out.println(name + "=" + reply.getObject(name));
}
```
An example output from the code above is shown below:
```
selector=null  
dispatchedQueueSize=1  
maximumPendingMessageLimit=0  
exclusive=false  
connectionId=ID:dejan-bosanacs-macbook-pro-2.local-64989-1335528942875-4:1  
destinationName=Test.Queue  
clientId=ID:dejan-bosanacs-macbook-pro-2.local-64989-1335528942875-3:1  
slowConsumer=false  
prefetchSize=1000  
sessionId=1  
dequeueCounter=0  
enqueueCounter=1  
retroactive=false  
dispatchedCounter=1
```