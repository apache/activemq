---
title: "New Features in 6.0"
slug: /new-features-in-60
---

ActiveMQ 6 modernizes the ActiveMQ 5 broker engine to support new JDK releases and provide support for Jakarta EE. This includes _partial_ support for JMS 2.0 APIs that are available in Jakarta Messaging 3.1.

This release includes performance improvements, enhancements for observability, and reduction in 3rd-party dependencies.

### Jakarta 3.1 and JMS 2.0 Support

ActiveMQ 6 supports Jakarta EE namespaces for both client and server. For details on the transition and the status of support for Jakarta Messaging 3.1 and JMS 2.0 API features, see the [Jakarta/JMS 2 transition page](jms2).

### Requirements

ActiveMQ 6 requires JDK 17 and uses Spring 6 on the broker-side for configuration.

### New Features
 
* Jakarta EE namespace support
* Shared user and password store for messaging and web components (shared JAAS Realm)
* Docker image improvements
* New JMX attributes on Connection
* Queue expiry check is skipped when queue is paused
* Jakarta Messaging 3.1/JMS 2.0 support updates: XA transaction and consumer `receiveBody(Class<T>)` methods
* Reduce inflight transaction memory footprint in KahaDB
* Includes security fix for CVE-2023-46604

### Key Dependency updates

* JDK 17, 21 supported
* Spring 6
* Jetty 11.x
* Apache Camel 4.x
* Jolokia 2.x

### Deprecated and Removed features

* Solaris support
* 32-bit support has been removed
* Removed unnecessary dependencies (jetty-continuation, commons-collections)
* Removed deprecated 4.x era JournalPersistenceAdapter (JDBCPersistenceAdapter has been default)
