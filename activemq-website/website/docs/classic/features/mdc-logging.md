---
title: "MDC Logging"
slug: /mdc-logging
---


ActiveMQ uses [slf4j](http://www.slf4j.org/) as its logging framework. This allows ActiveMQ to support [MDC logging](http://www.slf4j.org/api/org/slf4j/MDC.html). For more information about about MDC logging see the [logback manual](http://logback.qos.ch/manual/mdc.html).

Currently ActiveMQ adds three keys to the diagnostic context:

Key|Description
---|---
`activemq.broker`|Name of the broker logging event.
`activemq.connector`|Address of the connector which is serving the request.
`activemq.destination`|Name of the target destination of a message.

For example, if you've defined a log appender to use the following `ConversionPattern`:
```
log4j.appender.stdout.layout.ConversionPattern=%d [%-15.15t] - %-5p %-30.30c{1} - %-10.10X{activemq.broker} %-20.20X{activemq.connector} %-20.20X{activemq.destination} - %m%n
```
then in an environment where multiple brokers use the same log (like unit tests), the log output will take this general form:
```
2011-03-23 13:51:04,919 [127.0.0.1:58146] - TRACE Queue                          - broker-3B  tcp://localhost:6111                      - Message ID:dejan-bosanacs-macbook-pro....
2011-03-23 13:51:04,935 [oker-3B] Task-1] - DEBUG Queue                          - broker-3B  tcp://localhost:6111 PROD.FUSESOURCE.3.B  - PROD.FUSESOURCE.3.B toPageIn: 1, Inflight...
2011-03-23 13:51:04,763 [oker-2A] Task-1] - TRACE AbstractStoreCursor            - broker-2A  vm://broker-2A       PROD.FUSESOURCE.3.B  - QueueStorePrefetch611939300 - fillBatch
2011-03-23 13:51:04,759 [127.0.0.1:58118] - TRACE Queue                          - broker-3A  tcp://localhost:6110                      - Message ID:dejan-bosanacs-macbook-pro.l...
2011-03-23 13:51:04,937 [oker-2A] Task-1] - TRACE NetworkBridgeFilter            - broker-2A  vm://broker-2A       PROD.FUSESOURCE.3.B  - Message all ready routed once through t...
2011-03-23 13:51:04,936 [oker-3B] Task-1] - TRACE AbstractStoreCursor            - broker-3B  tcp://localhost:6111 PROD.FUSESOURCE.3.B  - QueueStorePrefetch2047424752 - fillBatch
2011-03-23 13:51:04,941 [oker-2A] Task-2] - DEBUG DemandForwardingBridgeSupport  - broker-2A                                            - bridging (broker-2A -> broker-1B, consu...
```
