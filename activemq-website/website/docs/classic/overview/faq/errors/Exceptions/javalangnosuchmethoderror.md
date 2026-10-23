---
title: "java.lang.NoSuchMethodError"
slug: /javalangnosuchmethoderror
---

If you get an error like
```
java.lang.NoSuchMethodError:
javax.jms.Session.createConsumer(Ljavax/jms/Destination;)Ljavax/jms/MessageConsumer;
```
or
```
java.lang.AbstractMethodError: 
javax.jms.ConnectionFactory.createConnection()Ljavax/jms/Connection;
```

### Fix

You probably have old JMS 1.0.2b versions of the JMS API on your classpath. This is because various methods were added in JMS 1.1 to work with destinations in a polymorphic way (such as to create a consumer from a Destination like the above stack trace).

Please add the JMS 1.1 jars to your container/classpath and try again.

Add to the correct ClassPath

If the JMS 1.0.2b classes come from your application server you may need to add the JMS 1.1 to your boot classpath to ensure that the JMS 1.1 classes take precedence to the container supplied classes

Note that to a user of the JMS API, JMS 1.1 is completely backwards compatible. The only issue with upgrading to JMS 1.1 is that any JMS provider which implements only JMS 1.0.2b and does not implement JMS 1.1 will not work; however if you're using ActiveMQ you should be fine.

