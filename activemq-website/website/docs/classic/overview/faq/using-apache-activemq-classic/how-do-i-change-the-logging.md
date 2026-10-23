---
title: "How do I change the logging"
slug: /how-do-i-change-the-logging
---

How do I change the logging
---------------------------

We use slf4j to log information in the broker client and the broker itself so you can fully configure which logging levels are used and whether to log to files or the console etc. For more information see the [log4j manual](http://logging.apache.org/log4j/docs/manual.html).

You can disable the ActiveMQ logging by editing the **conf/log4j.properties** file to disable all INFO level logging for activemq by adding this line
```
log4j.logger.org.apache.activemq=WARN
```
or to disable stdout logging all together change this line
```
log4j.rootLogger=INFO, stdout, out
```
to
```
log4j.rootLogger=INFO, out
```

How Do I Change the Logging at Runtime?
---------------------------------------

The default logging level in ActiveMQ is INFO. This can be seen by starting up the default ActiveMQ binary in a terminal. In doing so, you will see the output shown below:
```
$ ./bin/activemq
ACTIVEMQ_HOME: /Users/bsnyder/amq/apache-activemq-5.1.0
ACTIVEMQ_BASE: /Users/bsnyder/amq/apache-activemq-5.1.0
Loading message broker from: xbean:activemq.xml
INFO  BrokerService                  - Using Persistence Adapter: AMQPersistenceAdapter(/Users/bsnyder/amq/apache-activemq-5.1.0/data)
INFO  BrokerService                  - ActiveMQ 5.1.0 JMS Message Broker (localhost) is starting
INFO  BrokerService                  - For help or more information please see: http://activemq.apache.org/
INFO  AMQPersistenceAdapter          - AMQStore starting using directory: /Users/bsnyder/amq/apache-activemq-5.1.0/data
INFO  KahaStore                      - Kaha Store using data directory /Users/bsnyder/amq/apache-activemq-5.1.0/data/kr-store/state
INFO  AMQPersistenceAdapter          - Active data files: []
INFO  KahaStore                      - Kaha Store using data directory /Users/bsnyder/amq/apache-activemq-5.1.0/data/kr-store/data
INFO  TransportServerThreadSupport   - Listening for connections at: tcp://mongoose.local:61616
INFO  TransportConnector             - Connector openwire Started
INFO  TransportServerThreadSupport   - Listening for connections at: ssl://mongoose.local:61617
INFO  TransportConnector             - Connector ssl Started
INFO  TransportServerThreadSupport   - Listening for connections at: stomp://mongoose.local:61613
INFO  TransportConnector             - Connector stomp Started
INFO  TransportServerThreadSupport   - Listening for connections at: xmpp://mongoose.local:61222
INFO  TransportConnector             - Connector xmpp Started
INFO  NetworkConnector               - Network Connector default-nc Started
INFO  BrokerService                  - ActiveMQ JMS Message Broker (localhost, ID:mongoose.local-56138-1224025139259-0:0) started
INFO  log                            - Logging to org.slf4j.impl.JCLLoggerAdapter(org.mortbay.log) via org.mortbay.log.Slf4jLog
INFO  log                            - jetty-6.1.9
INFO  WebConsoleStarter              - ActiveMQ WebConsole initialized.
INFO  /admin                         - Initializing Spring FrameworkServlet 'dispatcher'
INFO  log                            - ActiveMQ Console at http://0.0.0.0:8161/admin
INFO  log                            - ActiveMQ Web Demos at http://0.0.0.0:8161/demo
INFO  log                            - RESTful file access application at http://0.0.0.0:8161/fileserver
INFO  log                            - Started SelectChannelConnector@0.0.0.0:8161
INFO  FailoverTransport              - Successfully connected to tcp://localhost:61616
```
But stopping ActiveMQ to change the logging level is not always feasible. There are many cases where the logging level and other configuration details may need to be changed at runtime. Using a JMX tool such as jconsole, the logging configuration file can be changed and reloaded without shutting down ActiveMQ.

To change the ActiveMQ logging level from INFO to DEBUG while ActiveMQ is running, start up jconsole, point to the ActiveMQ instance and navigate to the Broker object's Operations tab as shown below:

![](/img/reloadLog4jProperties1.png)

To enable debug level logging in ActiveMQ, edit the `conf/log4j.properties` file that is part of the ActiveMQ binary distribution to disable INFO level logging and enable DEBUG level logging from this:
```
log4j.rootLogger=INFO, stdout, out
log4j.logger.org.apache.activemq.spring=WARN
log4j.logger.org.springframework=WARN
log4j.logger.org.apache.xbean.spring=WARN

# When debugging or reporting problems to the ActiveMQ team,
# comment out the above lines and uncomment the next.

#log4j.rootLogger=DEBUG, out, stdout
```
to this:
```
#log4j.rootLogger=INFO, stdout, out
log4j.logger.org.apache.activemq.spring=WARN
log4j.logger.org.springframework=WARN
log4j.logger.org.apache.xbean.spring=WARN

# When debugging or reporting problems to the ActiveMQ team,
# comment out the above lines and uncomment the next.

log4j.rootLogger=DEBUG, out, stdout
```
Notice that the line `log4j.rootLogger=INFO, stdout, out` has been commented out and the line `log4j.rootLogger=DEBUG, out, stdout` has been uncommented. Save this change and then, in jconsole, click the button named **reloadLog4jProperties** as shown below:

![](/img/reloadLog4jProperties2.png)

After clicking the **reloadLog4jProperties** button, the dialog pops up stating, 'Method successfully invoked'. This just lets you know that the `reloadLog4jProperties` method was invoked to reload the `conf/log4j.properties` file. Now notice in the terminal where ActiveMQ is running that the logging that is being output is now DEBUG:
```
INFO  WebConsoleStarter              - ActiveMQ WebConsole initialized.
INFO  /admin                         - Initializing Spring FrameworkServlet 'dispatcher'
INFO  log                            - ActiveMQ Console at http://0.0.0.0:8161/admin
INFO  log                            - ActiveMQ Web Demos at http://0.0.0.0:8161/demo
INFO  log                            - RESTful file access application at http://0.0.0.0:8161/fileserver
INFO  log                            - Started SelectChannelConnector@0.0.0.0:8161
INFO  FailoverTransport              - Successfully connected to tcp://localhost:61616
DEBUG InactivityMonitor              - 10000 ms elapsed since last write check.
DEBUG InactivityMonitor              - 10000 ms elapsed since last write check.
DEBUG AbstractRegion                 - Removing consumer: ID:mongoose.local-56517-1224026019987-0:0:-1:1
DEBUG AbstractRegion                 - Removing consumer: ID:mongoose.local-56511-1224026011897-0:0:1:1
DEBUG TransportConnection            - Stopping connection: /127.0.0.1:56518
DEBUG TcpTransport                   - Stopping transport tcp:///127.0.0.1:56518
DEBUG TransportConnection            - Stopped connection: /127.0.0.1:56518
DEBUG TransportConnection            - Connection Stopped: /127.0.0.1:56518
DEBUG AbstractRegion                 - Removing consumer: ID:mongoose.local-56511-1224026011897-0:0:-1:1
DEBUG TransportConnection            - Stopping connection: /127.0.0.1:56512
DEBUG TcpTransport                   - Stopping transport tcp:///127.0.0.1:56512
DEBUG TransportConnection            - Stopped connection: /127.0.0.1:56512
DEBUG TransportConnection            - Connection Stopped: /127.0.0.1:56512
DEBUG InactivityMonitor              - 9999 ms elapsed since last write check.
DEBUG InactivityMonitor              - 10000 ms elapsed since last write check.
DEBUG InactivityMonitor              - 29999 ms elapsed since last read check.
```
Disabling `DEBUG` level logging and enabling `INFO` level logging is done by editing the `log4j.properties` file and clicking the **reloadLog4jProperties** button again.

