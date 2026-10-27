---
title: "How can I add a new type of transport"
slug: /how-can-i-add-a-new-type-of-transport
---

ActiveMQ is specifically designed to allow custom transports to be plugged in. You can programatically create TransportConnector instances and add them to the BrokerService in Java code using the [BrokerService.addConnector() method](http://activemq.codehaus.org/maven/apidocs/org/apache/activemq/broker/BrokerService.html#addConnector(org.apache.activemq.broker.TransportConnector))

or you can use the transport discovery mechanism, so that folks can just use your new transport using the neat URI syntax.

The way it works is that the URI scheme (e.g. "vm") is used to find a text file on the classpath - [here are all the schemes provided](http://svn.apache.org/repos/asf/incubator/activemq/trunk/activemq-core/src/main/resources/META-INF/services/org/apache/activemq/transport/).

then the contents of the file is the class name of a Java class on the classpath which extends the [TransportFactory class](http://activemq.codehaus.org/maven/apidocs/org/apache/activemq/transport/TransportFactory.html).

Then once you've implemented a Transport and TransportFactory class, folks can use your new transport using a URI in the JMS client or broker XML config file using the URI using your new schema name. e.g. if you wrote a new _foo_ transport, you could use a URI of _foo://localhost?myParam=bar_

