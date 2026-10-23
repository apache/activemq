---
title: "Why do KahaDB log files remain after cleanup"
slug: /why-do-kahadb-log-files-remain-after-cleanup
---

Clean-up of un-referenced KahaDB journal log files **`data-<id>.log`** will occur every 30 seconds by default. If a data file is in-use it will not be cleaned up.

A data file may be in-use because:

1.  It contains a pending message for a destination or durable topic subscription
2.  It contains an ACK for a message which is in an in-use data file - the ACK cannot be removed as a recovery would then mark the message for redelivery
3.  The journal references a pending transaction
4.  It is a journal file, and there may be a pending write to it

The **`TRACE`** level logging of the **`org.apache.activemq.store.kahadb.MessageDatabase`** class provides insight into the cleanup process and will allow you to determine why a given data file is considered in-use and as a result, not a candidate for cleanup.

To debug, add the following (or similar) to your **`log4j.properties`** file (if needed):

log4j.appender.kahadb=org.apache.log4j.RollingFileAppender 
log4j.appender.kahadb.file=`${activemq.base}`/data/kahadb.log 
log4j.appender.kahadb.maxFileSize=1024KB 
log4j.appender.kahadb.maxBackupIndex=5 
log4j.appender.kahadb.append=true 
log4j.appender.kahadb.layout=org.apache.log4j.PatternLayout 
log4j.appender.kahadb.layout.ConversionPattern=%d \[%-15.15t\] %-5p %-30.30c{1} - %m%n 
log4j.logger.org.apache.activemq.store.kahadb.MessageDatabase=TRACE, kahadb

Either restart ActiveMQ and let the cleanup process run (give it a minute or two for example) or alternatively apply this logging configuration to a running broker via JMX. The **`Broker`** MBean exposes an operation called **`reloadLog4jProperties`** in JMX that can be used to tell the broker to reload its **`log4j.properties`**. Often its enough to apply this logging configuration for 2-5 minutes and then analyze the broker's log file.

Examine the log file and look for cleanup of the data files. The process starts with the complete set of known data files and queries the index on a per destination basis to prune this list. Anything that remains is a candidate for cleanup. The trace logging gives the destination and the log file numbers that remain candidates for removal as it iterates through the index.

At some point you'll hit a destination and the number of data file ids will suddenly drop because that destination references them. It could be a DLQ or an offline durable subscriber. In any event, the logging will help you pinpoint the destinations that are hogging disk space.

Here is a quick sample:

 TRACE | Last update: 164:41712, full gc candidates set: \[86, 87, 163, 164\] | org.apache.activemq.store.kahadb.MessageDatabase | ActiveMQ Journal Checkpoint Worker
 TRACE | gc candidates after first tx:164:41712, \[86, 87, 163\] | org.apache.activemq.store.kahadb.MessageDatabase | ActiveMQ Journal Checkpoint Worker
 TRACE | gc candidates after dest:0:A, \[86, 87, 163\] | org.apache.activemq.store.kahadb.MessageDatabase | ActiveMQ Journal Checkpoint Worker
 TRACE | gc candidates after dest:1:B, \[86, 87, 163\] | org.apache.activemq.store.kahadb.MessageDatabase | ActiveMQ Journal Checkpoint Worker
 TRACE | gc candidates after dest:0:D, \[86, 87, 163\] | org.apache.activemq.store.kahadb.MessageDatabase | ActiveMQ Journal Checkpoint Worker
 TRACE | gc candidates after dest:0:E, \[86, 87\] | org.apache.activemq.store.kahadb.MessageDatabase | ActiveMQ Journal Checkpoint Worker
 TRACE | gc candidates after dest:0:H, \[86, 87\] | org.apache.activemq.store.kahadb.MessageDatabase | ActiveMQ Journal Checkpoint Worker
 TRACE | gc candidates after dest:0:I, \[86, 87\] | org.apache.activemq.store.kahadb.MessageDatabase | ActiveMQ Journal Checkpoint Worker
 TRACE | gc candidates after dest:0:J, \[87\] | org.apache.activemq.store.kahadb.MessageDatabase | ActiveMQ Journal Checkpoint Worker
 TRACE | gc candidates: \[87\] | org.apache.activemq.store.kahadb.MessageDatabase | ActiveMQ Journal Checkpoint Worker
 DEBUG | Cleanup removing the data files: \[87\] | org.apache.activemq.store.kahadb.MessageDatabase | ActiveMQ Journal Checkpoint Worker

We get one candidate, **`data-87.log`** from the existing set of journal data files **`[86, 87, 163, 164]`**. There is a current transaction using **`164`**, destination (Queue named **`E`**) `'**0:E**'` has some messages in **`163`**, destination `'**0:I**'` has messages in **`86`** and **`87`** is un-referenced. In this case, there must be some long standing un-acknowledged messages or a very slow consumer on destination `'**0:I**'`.

The `'**0:**'` prefix is shorthand for a queue, `'**1:**'` for a topic. Example: **`dest:1:B`** refers to a topic named **`B`**.

* * *

### Non-persistent messages

Similar for non-persistent messages that are not stored in your configured KahaDB persistence adapter but get swapped to temp storage once they exceed the broker's configured **`memoryUsage`** limit. A similar logging configuration can show details of the cleanup of temp storage.

log4j.appender.kahadb=org.apache.log4j.RollingFileAppender
log4j.appender.kahadb.file=`${activemq.base}`/data/kahadb.log
log4j.appender.kahadb.maxFileSize=1024KB
log4j.appender.kahadb.maxBackupIndex=5
log4j.appender.kahadb.append=true
log4j.appender.kahadb.layout=org.apache.log4j.PatternLayout
log4j.appender.kahadb.layout.ConversionPattern=%d \[%-15.15t\] %-5p %-30.30c{1} - %m%n
log4j.logger.org.apache.activemq.store.kahadb=TRACE, kahadb
log4j.logger.org.apache.activemq.store.kahadb.MessageDatabase=INFO, kahadb
 

Note the last line of above logging configuration disables the verbose logging of the KahaDB cleanup task. If that line gets removed, the cleanup details of both KahaDB and temp storage will be logged to the same file but you need to be careful not to mix the logging output.

