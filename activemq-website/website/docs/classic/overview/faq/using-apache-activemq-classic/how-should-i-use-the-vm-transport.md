---
title: "How should I use the VM transport"
slug: /how-should-i-use-the-vm-transport
---

### For ActiveMQ 3.x/4.x

[Using the VM transport](how-do-i-use-activemq-classic-using-in-jvm-messaging) to connect to an in-JVM broker is the fastest and most efficient transport you can use.

This is because by default there is no serialization to a socket or operating system socket resources used up; its purely an in-JVM List used to exchange messages with clients and the broker. Sometimes you want the VM transport to work as an async SEDA queue; other times you want to inline the processing so that there are less thread context switches which can improve throughput in high performance scenarios.

One thing to note with the VM transport is that messages are passed by reference. One slight exception to this is the JMS ObjectMessage. (see below for details on how to disable it in ActiveMQ 4.x)

You can also further optimise things by setting the **copyMessageOnSend** property to be false; which avoids making a copy of the ObjectMessage to send; though this assumes that you don't try and resuse the ObjectMessage instance; you just create it once and send it once and don't try and change the body of the message after sending it.

Be careful of ClassLoaders

Note that you should only perform the above optimisation if all the producers and consumers are in a compatible class loader. For example if you have 2 WARs with their own jars which are sending messages to each other, you could get strange ClassCastExceptions occuring due to the same class being loaded in each class loader; so sometimes serializing ObjectMessage is a good thing to avoid ClassPath hell - so only perform the above optimisation if you are sure you're ClassPaths are OK

### Disabling Object serialization with ObjectMessage for ActiveMQ 4.x

The JMS specification dictates that the body of an ObjectMessage must be serialized when you call send() to avoid the object changing under your feet affecting what view the consumer sees of the object.

You can disable the automatic serialization of ObjectMessage payloads so that the objects are passed by value in 4.x by setting the **objectMessageSerializationDefered** flag to true on the ActiveMQConnectionFactory (or ActiveMQConnection).
```
ActiveMQConnectionFactory factory = new ActiveMQConnectionFactory("vm://localhost");
factory.setObjectMessageSerializationDefered(true);
```
