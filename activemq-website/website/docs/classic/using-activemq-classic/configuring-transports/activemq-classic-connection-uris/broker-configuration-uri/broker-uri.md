---
title: "Broker URI"
description: "Use the broker: URI syntax to start an ActiveMQ Classic broker with transport and network connectors plus options such as persistent, useJmx and brokerName."
slug: /broker-uri
---


Broker URI
----------

The Broker URI allows you to [run a configured broker](run-broker) using a single URI for all the configuration.

#### Syntax

The URI is assumed to be a composite uri with multiple uris that are used to bind the connectors of the broker.
```
broker:(transportURI,network:networkURI)/brokerName?brokerOptions
```
If a composite URI use the network: scheme, then the rest of the URI is bound as a network connector.

##### Broker Options

Option Name|Default Value|Description
---|---|---
useJmx|rue|Should the broker be exposed to JMX?
persistent|true|Should the broker use persistent storage
populateJMSXUserID|false|Should the broker populate the [JMSXUserID](jmsxuserid) property of messages to indicate the authenticated sender username who sent the message
useShutdownHook|true|should the broker install a shutdown hook so that it can properly shut itself down on a JVM kill
brokerName|localhost|The name of the broker
deleteAllMessagesOnStartup|false|Should all the messages in the persistent store be deleted on broker startup
enableStatistics|true|Should statistics gathering be enabled

##### Example URI

The following example starts up a broker accepting connections on port 61616, and establishes a network connection to remotehost:61616 and disables persistence.
```
broker:(tcp://localhost:61616,network:static:tcp://remotehost:61616)?persistent=false&useJmx=true
```
You can use the Broker URI on other transports

If you are using another transport - such as the [VM Transport Reference](vm-transport-reference) then you can refer to the above broker URI properties inside that URL. e.g. using the URL
```
vm://localhost?broker.persistent=false
```
