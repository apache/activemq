---
title: "JMS Streams"
slug: /jms-streams
---


> **DEPRECATED**

This feature is deprecated, and end users is encouraged to **not** use it. This feature will be removed in a later ActiveMQ release.

Sometimes you need to send truly massive files (many Gb) around the network in a reliable manner. The JMS API expects JMS clients to be able to keep a message in memory at one time, so sending > 1Gb messages around ends up using way too much RAM on the client side.

To solve this problem ActiveMQ supports regular [InputStream](http://java.sun.com/j2se/1.4.2/docs/api/java/io/InputStream.html) and [OutputStream](http://java.sun.com/j2se/1.4.2/docs/api/java/io/OutputStream.html) abstractions which work with regular JMS producers and consumers.

This allows you to use the familar streams from Java to send or receive messages of any size at all (providing your file system can handle them while keeping a low memory overhead.

For Users of 4.2

If you are using 4.2 onwards of ActiveMQ we highly recommend you try out [Blob Messages](blob-messages) which offers a more flexible mechanism for dealing wtih massive files and fully supports out-of-band transfer using HTTP/FTP as well as allowing external resources to be sent around the JMS network.

### Using JMS Streams

To use the streams just create an input or output stream depending on if you are reading or writing using the [connection.createInputStream()](http://incubator.apache.org/activemq/maven/activemq-core/apidocs/org/apache/activemq/ActiveMQConnection.html#createInputStream(javax.jms.Destination)) or  
[connection.createOutputStream()](http://incubator.apache.org/activemq/maven/activemq-core/apidocs/org/apache/activemq/ActiveMQConnection.html#createOutputStream(javax.jms.Destination)) methods.

e.g.
```
ActiveMQConnection connection = ...;
Destination destination = new ActiveMQQueue("FOO.BAR");

OutputStream out = connection.createOutputStream(destination);

// write the file to out
out.close();
```
Or to consume a large message
```
ActiveMQConnection connection = ...;
Destination destination = new ActiveMQQueue("FOO.BAR");

InputStream in = connection.createInputStream(destination)

// read the stream...
in.close();
```
There are overloaded createInputStream/createOutputStream methods which support additional paramateres to be passed.

For further reference see the javadoc.

*   [ActiveMQInputStream](http://activemq.apache.org/maven/activemq-core/apidocs/org/apache/activemq/ActiveMQInputStream.html)
*   [ActiveMQOutputStream](http://activemq.apache.org/maven/activemq-core/apidocs/org/apache/activemq/ActiveMQOutputStream.html)

_Note:_  
The counterpart classes in AMQ 3.x are :

*   JMSInputStream
*   JMSOutputStream

