---
title: "Scaling Queues"
slug: /scaling-queues
---


Scaling to tens of thousands of Queues in a single broker is relatively straightforward - but requires some configuration changes from the default.

Reducing Threads
----------------

With the default configuration, ActiveMQ is configured to use a dispatch thread per Queue - you can use set the optimizedDispatch property on the destination policy entry - see [configuring Queues](per-destination-policies).

ActiveMQ can optionally use internally a thread pool to control dispatching of messages - but as a lot of deployment operating systems are good at handling a large number of threads, this is off by default. To enable this option, either set the ACTIVEMQ_OPTS to disable dedicated task runners in the start up script, INSTALL\_DIR/bin/activemq -e.g.
```
ACTIVEMQ_OPTS="-Xmx512M -Dorg.apache.activemq.UseDedicatedTaskRunner=false"  
```
or you can set ACTIVEMQ_OPTS in /etc/activemq.conf.

**Note:** From ActiveMQ 5.6 onwards the dedicated task runner is disabled by default (see [AMQ-3667](https://issues.apache.org/jira/browse/AMQ-3667)).

To reduce the number of threads used for the transport - take a look at using the NIO transport - see [Configuring Transports](configuring-transports)

Here is an [example](http://svn.apache.org/repos/asf/activemq/trunk/assembly/src/sample-conf/activemq-scalability.xml) of this in use in one of the provided sample broker configuration files.

Reducing Memory Consumption
---------------------------

Reduce the memory used per thread - see [reducing memory consumption](javalangoutofmemory)

Reduce number of file descriptors
---------------------------------

ActiveMQ uses the amqPersistenceAdapter by default for persistent messages. Unfortunately, this persistence adapter (as well as the kahaPersistenceAdapter) opens a file descriptor for each queue. When creating large numbers of queues, you'll quickly run into the limit for your OS.

You can either choose another [persistence option](persistence)

or - try out the new [KahaDB](kahadb) in version 5.3 and higher

Increase the limit on file descriptors per process
--------------------------------------------------

Try [googling for the OS you are using](http://tinyurl.com/o9qs2f)

