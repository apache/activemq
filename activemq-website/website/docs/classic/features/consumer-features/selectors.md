---
title: "Selectors"
slug: /selectors
---


JMS Selectors
-------------

Selectors are a way of attaching a filter to a subscription to perform content based routing. Selectors are defined using SQL 92 syntax and typically apply to message headers; whether the standard properties available on a JMS message or custom headers you can add via the JMS code.

Here is an example
```
JMSType = 'car' AND color = 'blue' AND weight > 2500
```
For more documentation on the detail of selectors see the reference on [javax.jmx.Message](http://java.sun.com/j2ee/1.4/docs/api/javax/jms/Message.html).  
ActiveMQ supports some JMS defined properties, as well as some ActiveMQ ones - see [message properies](activemq-classic-message-properties) \- that the selector can use.

### Using XPath to filter messages

Apache ActiveMQ also supports XPath based selectors when working with messages containing XML bodies. To use an XPath selector use the following syntax
```
XPATH '//title\[@lang=''eng''\]'
```

**Xalan dependency**

XPath support requires [Xalan](http://xalan.apache.org/index.html) which is not part of the distribution. The Xalan jars needs to be manually added to lib directory or pulled into your app via maven
```
<dependency>
    <groupId>xalan</groupId>
    <artifactId>xalan</artifactId>
    <version>2.6.0</version>
</dependency>
```

### String Property Conversions / Selecting Against STOMP messages

The JMS spec states that a String property should not get converted to a numeric when used in a selector. So for example, if a message has the 'age' property set to String '21' then the following selector should not match it: 'age > 18'. Since ActiveMQ support STOMP client which can only send messages with string properties, that restriction is a bit limiting. If you want your JMS selectors to auto-convert String properties the the appropriate number type, just prefix the the selector with 'convert\_string\_expressions:'. If you changed selector in the previous example to be 'convert\_string\_expressions:age > 18', then it would match the message.

