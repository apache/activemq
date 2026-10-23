---
title: "log4j-WARN No appenders could be found for logger"
slug: /log4j-warn-no-appenders-could-be-found-for-logger
---

Error
-----
```
log4j:WARN No appenders could be found for logger
(org.activemq.transport.tcp.TcpTransportChannel).
log4j:WARN Please initialize the log4j system properly.
```

Solution
--------

You have not initialised log4j properly. Try reading the [online log4j manual](http://logging.apache.org/log4j/docs/manual.html) or by adding a log4j.properties file to your classpath (to a directory which is on the classpath to be precise).

