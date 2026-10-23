---
title: "Running a 3.x Broker"
slug: /running-a-3x-broker
---

The 3.x [binary distribution](/components/classic/download) of ActiveMQ comes with a script called 'activemq' which allows you to run a broker. Typing the following will run an ActiveMQ Broker using the out of the box configuration

```
activemq
```

You can use the [Xml Configuration](xml-configuration) to customize the Message Broker to suit your needs. You can run a broker with a specific XML configuration as

```
activemq foo.xml
```

### Running the broker inside a Servlet Engine

If you have the binary distribution (3.1 onwards) you can run the broker using a WAR distribution. From the binary download type the following

```
cd example ant war
```

and you get a war in the target directory capable of being deployed in a servlet engine. This has been tried in Resin, Tomcat 4.x, 5.x, Jetty 5.x and should work in most good servlet engines. See the Ant build for details and web.xml of how this works if you wish to integrate this into your WAR.

### Running the broker inside your J2EE 1.4 Application Server

Whether its Apache Geronmio, JBoss 4, WebLogic 9 or some other J2EE 1.4 container you should be able to just deploy the activemq-*.rar which is included in the binary distribution as a deployment unit in your app server.

### Running the 3.x broker from the source code

You can do this by running the main() in the [org.activemq.broker.impl.Main](http://activemq.codehaus.org/maven/apidocs/org/activemq/broker/impl/Main.html) class inside your IDE, assuming you've [set up your classpath](initial-configuration) properly.

Another option, if you have a [source distribution](building), is to run the broker from inside Maven by typing

```
cd modules/assembly maven server
```

You can specify a URL to listen on by specifying a parameter, such as

```
java org.activemq.broker.impl.Main tcp://localhost:61626
```

Or via Maven

```
maven server -Durl=tcp://localhost:61626
```

### Using the XML configuration

You can use the [Xml Configuration](xml-configuration) to customize the Message Broker to suit your needs. You can run a broker from Maven as follows...

```
maven server -Dconfig=src/sample-conf/default.xml
```

If your [classpath is setup correctly](initial-configuration) you can achieve the same thing from the command line

```
java org.activemq.spring.Main src/sample-conf/default.xml
```

Notice that we supply an alternative Main which is dependent on Spring and takes an XML configuration file as the argument.

Handling JMS brokers going down
-------------------------------

A common requirement is that if the JMS broker goes down you want to automatically detect the failure and try to reconnect under the covers so that your application does not have to worry about reconnection.

There is detailed documentation on this in [Configuring Transports](configuring-transports); briefly...

Just change your connection URI in 3.x to

```
reliable:tcp://host:port
```
