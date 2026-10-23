---
title: "New Features in 5.5"
slug: /new-features-in-55
---


New Features in 5.5.0
---------------------

*   Dependency on Java 1.6 (java 1.5 is no longer supported)
*   MDC logging ([SLF4J](http://slf4j.org/))
*   Upgrade to camel 2.7.0
*   DLQ processing per durable subscription
*   New network connector MBeans
*   [IOExceptionHandler](configurable-ioexception-handling) for JDBC store
*   Added support for [Apache Commons Daemon](http://commons.apache.org/daemon/)

Improvements in 5.5.0
---------------------

*   Improved support for recovery of durable subscribers with priority support
*   Improved performance for offline durable subscriptions with large message backlogs (JDBC store)
*   better support for Guest login via JAAS

