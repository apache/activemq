---
title: "Command Agent"
slug: /command-agent
---


Removed in 5.9

*   [https://issues.apache.org/jira/browse/AMQ-4406](https://issues.apache.org/jira/browse/AMQ-4406)

Command Agent
-------------

From 4.2 onwards Apache ActiveMQ allows you to communicate with a broker Command Agent to be able to perform administration queries and commands. This allows you to use the message bus itself to communicate with the message broker to list available queues, topics, subscriptions, to view metadata, browse queues and so forth.

### Enabling the Command Agent

To enable the command agent, in the activemq.xml (or your spring.xml you use to configure the broker) add the following
```
<beans>
  <broker useJmx="true" xmlns="http://activemq.apache.org/schema/core">
    ...
        <managementContext>
            <managementContext createConnector="true"/>
        </managementContext>
    ...
  </broker>

  <commandAgent xmlns="http://activemq.apache.org/schema/core"/>
  ...
</beans>
```
The broker will then have a command agent connected which will listen on the topic **ActiveMQ.Agent**.

### Using Jabber (XMPP) to talk to the Broker

You can use the [XMPP](xmpp) support to talk with the broker. For example follow the instructions in [XMPP](xmpp) to connect via a Jabber client, then just join the room **ActiveMQ.Agent** and talk to the broker!

The following image shows the [Spark](http://jivesoftware.com/products/spark/) client in action talking to the broker

![](/img/help.png)

### Using the interactive console

The activemq-core module has a simple Java class called SimpleConsole which you can run in your IDE to get an interactive console for communicating with a broker over JMS. Type in the various commands (or help to get more help) and the results are returned on the console.

