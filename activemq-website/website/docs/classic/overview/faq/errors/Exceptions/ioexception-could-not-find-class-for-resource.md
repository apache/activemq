---
title: "IOException - could not find class for resource"
slug: /ioexception-could-not-find-class-for-resource
---

If you get an exception looking like this
```
Reason:  java.io.exception : could not find class for resource: META-INF/services/org/apache/activemq/transport/tcp
```

### Cause

You are probably using the ActiveMQ source code without using the resources

### Quick fix

Try one of these

*   use one of the distribution jars for ActiveMQ
*   use Maven to run your program
*   try adding activemq/src/conf to your classpath

### Background

Then it means that the files in META-INF/services could not be found on the classpath. These files are used to support loose coupling on ActiveMQ with the transport protocols (e.g. to avoid a classpath dependency on JXTA) and to allow dynamic protocol enhancement without a change to the core.

So we're using the META-INF/services files as a way of coupling a protocol used in URL connections to a Java class name.

