---
title: "Audit Logging"
description: "Turn on ActiveMQ Classic audit logging with the org.apache.activemq.audit property to record JMX and Web Console management actions, users and IP addresses."
slug: /audit-logging
---


For many secured environments there's a requirement to log every user management action. For that ActiveMQ implements _audit logging_, which means that every management action made through JMX or Web Console management interface will be logged and available for later inspection.

Audit logging comes pre-configured with the distribution, so it's very easy to turn it on or off. All you have to do is to set `org.apache.activemq.audit` system property.  From 5.16.0 the value can be one of "true|entry|exit|all". When the value is all or exit, the audit captures the time the JMX operation completed. You can do that by uncommenting the following line in the startup script:
```
ACTIVEMQ_OPTS="$ACTIVEMQ_OPTS -Dorg.apache.activemq.audit=true"
```
The actual logs are by default stored in `${ACTIVEMQ_HOME}/data/audit.log` and for secured broker you may expect entries similar to the following:
```
2010-12-22 12:12:07,225 | INFO  | admin requested /admin/createDestination.action [JMSDestination='test' JMSDestinationType='queue' secret='4eb0bc3e-9d7a-4256-844c-24f40fda98f1' ] from 127.0.0.1 | qtp12205619-39
2010-12-22 12:12:14,512 | INFO  | admin requested /admin/purgeDestination.action [JMSDestination='test' JMSDestinationType='queue' secret='eff6a932-1b58-45da-a64a-1b30b246cfc9' ] from 127.0.0.1 | qtp12205619-36
2010-12-22 12:12:17,802 | INFO  | admin requested /admin/sendMessage.action [JMSTimeToLive='' JMSXGroupSeq='' AMQ_SCHEDULED_DELAY='' JMSType='' JMSMessageCountHeader='JMSXMessageCounter' JMSXGroupID='' JMSReplyTo='' JMSDestination='test' AMQ_SCHEDULED_PERIOD='' JMSText='Enter some text 
here for the message body...' JMSDestinationType='queue' AMQ_SCHEDULED_CRON='' JMSCorrelationID='' AMQ_SCHEDULED_REPEAT='' JMSMessageCount='1' secret='a0e1df62-14d6-4425-82a2-17aa01a16e7d' JMSPriority='' ] from 127.0.0.1 | qtp12205619-37
...
2010-12-22 12:12:57,553 | INFO  | admin called org.apache.activemq.broker.jmx.QueueView.purge[] | RMI TCP Connection(8)-192.168.1.107
2010-12-22 12:13:21,976 | INFO  | admin called org.apache.activemq.broker.jmx.QueueView.resetStatistics[] | RMI TCP Connection(8)-192.168.1.107
2010-12-22 12:13:32,457 | INFO  | admin called org.apache.activemq.broker.jmx.QueueView.sendTextMessage[message] | RMI TCP Connection(6)-192.168.1.107
```
In this example you can see sample entries for actions taken both in Web Console or via JMX. Log entries contain info like:

*   username (if available), or "anonymous" otherwise
*   Operation performed, which in JMX case is the method name and request URL if the operation is performed over web
*   Parameters used for the operation and
*   IP address from which call has been made

A default location of the audit log can be configured in `${ACTIVEMQ_HOME}/conf/log4j.properties`

