---
title: "Examples"
description: "Older ActiveMQ examples page: the producer and consumer command line tools added in 5.12, activemq-all jar and Karaf commands, and Ant examples."
slug: /examples
displayed_sidebar: docs
---


Since version **5.12.0**, Apache ActiveMQ comes with the new tool that can be used to produce and consume messages from the broker.

Prerequisites
-------------

Before running the examples you should try [running a JMS broker](run-broker) on your machine. Follow the [Installation](installation) instructions to use a binary distribution of ActiveMQ. To run the broker in a command shell, type:
```
bin/activemq console
```
This starts up ActiveMQ.

Running
-------

From the [command line](unix-shell-script) you can run
```
${ACTIVEMQ_HOME}/bin/activemq producer
${ACTIVEMQ_HOME}/bin/activemq consumer
```
If you have `activemq-all` jar available you can achieve the same with
```
java -jar activemq-all-5.x.x.jar producer
java -jar activemq-all-5.x.x.jar consumer
```
If you run inside [Karaf](osgi-integration), you can run the commands as
```
activemq:producer
activemq:consumer
```

Options
-------

For all options on the commands, run them with `--help` parameter. The up to date, options for [producer](https://github.com/apache/activemq/blob/master/activemq-console/src/main/resources/org/apache/activemq/console/command/producer.txt) and [consumer](https://github.com/apache/activemq/blob/master/activemq-console/src/main/resources/org/apache/activemq/console/command/consumer.txt) can be found in the source.

Examples
--------

Here are a couple of examples of more advanced features.

To send a text message with custom text use
```
bin/activemq producer --message "My message" --messageCount 1
```
To send byte message of custom length use
```
bin/activemq producer --messageSize 100 --messageCount 1
```
To send a text message with content obtained from an url
```
bin/activemq producer --payloadUrl http://activemq.apache.org/schema/core/activemq-core.xsd --messageCount 1
```
To consume in transaction use
```
bin/activemq consumer --transacted true
```
To use client acknowledgment use
```
bin/activemq consumer --ackMode CLIENT_ACKNOWLEDGE
```
To use durable topic subscribers use
```
bin/activemq consumer --durable true --clientId example --destination topic://TEST
```

Old examples
------------

In older versions of ActiveMQ, the corresponding examples were located in `examples/` or `examples/openwire/swissarmy/` directories, where you can do the similar tasks with an ant script.
```
ant producer
ant consumer
```

Other examples
--------------

In `examples/` directory (depending on the version) you can find more examples of using the broker with variety of protocols (mqtt, amqp, ...) and clients (Java, Ruby, JavaScript, ...), so it's the good place to start learning. Also, `examples/conf/` directory contains a lot of different configuration examples that you can use as a starting point for your deployment.

See Also
--------

*   [Web Samples](web-samples)
*   [Web Console](web-console)
*   [JMX](jmx)

