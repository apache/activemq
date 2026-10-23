---
title: "java.io.IOException Failed to create database 'derbydb', see the next exception for details"
slug: /javaioioexception-failed-to-create-database-derbydb-see-the-next-exception-for-details
---

If you get an error like this
```
Jun 19, 2006 10:35:27 PM org.apache.activemq.broker.BrokerService getBroker
INFO: ActiveMQ 4.0 JMS Message Broker (localhost) is starting
Jun 19, 2006 10:35:27 PM org.apache.activemq.broker.BrokerService getBroker
INFO: For help or more information please see: http://incubator.apache.org/activemq/
java.io.IOException: Failed to create database 'derbydb', see the next exception for details.
       at org.apache.activemq.util.IOExceptionSupport.create(IOExceptionSupport.java:42)
       at org.apache.activemq.store.jdbc.TransactionContext.getConnection(TransactionContext.java:58)
       at org.apache.activemq.store.jdbc.JDBCPersistenceAdapter.createAdapter(JDBCPersistenceAdapter.java:229)
       at org.apache.activemq.store.jdbc.JDBCPersistenceAdapter.getAdapter(JDBCPersistenceAdapter.java:213)
       at org.apache.activemq.store.jdbc.JDBCPersistenceAdapter.start(JDBCPersistenceAdapter.java:139)
       at org.apache.activemq.store.journal.JournalPersistenceAdapter.start(JournalPersistenceAdapter.java:215)
       at org.apache.activemq.broker.BrokerService.createRegionBroker(BrokerService.java:930)
       at org.apache.activemq.broker.BrokerService.createBroker(BrokerService.java:888)
       at org.apache.activemq.broker.BrokerService.getBroker(BrokerService.java:458)
       at org.apache.activemq.broker.BrokerService.addConnector(BrokerService.java:143)
       at org.apache.activemq.broker.BrokerService.addConnector(BrokerService.java:133)
       at com.ic.ntn.message.HelloWorld$HelloWorldBroker.run(HelloWorld.java:92)
       at java.lang.Thread.run(Thread.java:595)
Caused by: SQL Exception: Failed to create database 'derbydb', see the next exception for details.
       at org.apache.derby.impl.jdbc.Util.newEmbedSQLException(Unknown Source)
       at org.apache.derby.impl.jdbc.Util.newEmbedSQLException(Unknown Source)
       at org.apache.derby.impl.jdbc.Util.generateCsSQLException(Unknown Source)
       at org.apache.derby.impl.jdbc.EmbedConnection.newSQLException(Unknown Source)
       at org.apache.derby.impl.jdbc.EmbedConnection.createDatabase(Unknown Source)
       at org.apache.derby.impl.jdbc.EmbedConnection.<init>(Unknown Source)
       at org.apache.derby.impl.jdbc.EmbedConnection30.<init>(Unknown Source)
       at org.apache.derby.jdbc.Driver30.getNewEmbedConnection(Unknown Source)
       at org.apache.derby.jdbc.InternalDriver.connect(Unknown Source)
       at org.apache.derby.jdbc.EmbeddedDataSource.getConnection(Unknown Source)
       at org.apache.derby.jdbc.EmbeddedDataSource.getConnection(Unknown Source)
       at org.apache.activemq.store.jdbc.TransactionContext.getConnection(TransactionContext.java:54)
       ... 11 more
```
Then the error is probably that the JDBC driver (Apache Derby by default) could not write to the persistent file area.

### Workaround

Create a directory for the broker to write its files. e.g. in Java code call [setDataDirectory](http://incubator.apache.org/activemq/maven/activemq-core/apidocs/org/apache/activemq/broker/BrokerService.html#setDataDirectory(java.io.File))
```
File dir = new File("foo");
dir.mkdir();
broker.setDataDirectory(dir);
```
or in XML use
```
<broker dataDirectory="foo">...
```

### See

*   [How do I embed a Broker inside a Connection](how-do-i-embed-a-broker-inside-a-connection)

