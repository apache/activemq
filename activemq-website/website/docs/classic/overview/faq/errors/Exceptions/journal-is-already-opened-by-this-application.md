---
title: "Journal is already opened by this application"
slug: /journal-is-already-opened-by-this-application
---

### Error

You get something like this
```
java.io.IOException: Journal is already opened by this application.
       at org.apache.activeio.journal.active.ControlFile.lock(ControlFile.java:71)
       at org.apache.activeio.journal.active.LogFileManager.initialize(LogFileManager.java:120)
       at org.apache.activeio.journal.active.LogFileManager.<init>(LogFileManager.java:101)
       at org.apache.activeio.journal.active.JournalImpl.<init>(JournalImpl.java:99)
       at org.apache.activemq.store.DefaultPersistenceAdapterFactory.createJournal(DefaultPersistenceAdapterFactory.java:198)
       at org.apache.activemq.store.DefaultPersistenceAdapterFactory.getJournal(DefaultPersistenceAdapterFactory.java:134)
```

### Description

Each broker needs to have its own directory to store its journal files etc. The error indicates that you have 2 brokers sharing the same files.

A common cause of this exception is that you are running two brokers on one machine using the same config file pointing to the same directory. So a work around is to parameterise the directory name via Spring's property syntax - or just create another configuration file so that the second broker uses a different directory.

Another cause of this problem is if you are using the vm://localhost style transport in a JMS client with the JMS connection starting before you have initialised your broker. If you create a vm transport connection, it will auto-create a broker if there is not one running already; so you can end up creating 2 brokers by accident. The work around is to make sure that the JMS connection factory you are using depends on the broker you are configuring (e.g. in Spring use a **depends-on** attribute on the connection factory to make it depend on the broker). This will ensure that the broker is initialized first before the connection factory.

Be careful with broker names and URIs

Make sure you do not use any strange characters in the names of brokers as they are converted to URIs which [do not allow things like underscores](http://java.sun.com/j2se/1.4.2/docs/api/java/net/URI.html) in them etc.

This problem could also be caused by [a bad OS and JVM combination](the-broker-will-not-start)

