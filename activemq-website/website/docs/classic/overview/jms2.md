---
title: "ActiveMQ Jakarta Messaging 3.1 & JMS 2.0 Support"
slug: /jms2
---

### Transition Approach

Initially, ActiveMQ clients will not support all JMS 2.0 functionality and will throw an `UnsupportedOperationException` (`RuntimeException`) for unsupported methods and features, such as those for async send with a `CompletionListener`, sending messages with a delivery delay, and using shared topic consumers.

As features are implemented in subsequent releases, these exceptions will be replaced with fully functional methods, examples and unit tests. See later/below for more details on implementation progress.

Until full producer DeliveryDelay support is implemented, ActiveMQ always returns zero (0) for getDeliveryDelay() to support the pool used by Spring JMS.

### Jakarta Messaging

Support for JMS 2.0 also enables building upon this for transition to Jakarta Messaging 3.1 and its new `jakarta.jms` API namespace rather than the historical `javax.jms` namespace.

 * The standard for Java Enterprise features going forward
 * Required for Spring 6
 * Required for Jakarta EE 9 + 10.
 * JMS API Java package name-change only
 * ActiveMQ package names do not change

This allows for a dependency-update-only approach to the transition without having to re-code going forward!

Client jar|API Package|Version|Notes
---|---|---|---
activemq-client | `jakarta.jms` | 6.0.x | Partial Jakarta Messaging 3.1 support. Works with embedded broker and VM transport.
activemq-client | `javax.jms` | 5.18.x | Partial JMS 2.0 support. Works with embedded broker and VM transport.
activemq-client-jakarta | `jakarta.jms` | 5.18.1+ | Transitional module for partial, *client-only* Jakarta Messaging 3.1 support. Can't be used in the same JVM as activemq-client or broker which use javax.jms API. **Note this means no embedded broker / VM transport support, only remote brokers.**

There is no functional change, this is purely a Java package naming change:
```java
import javax.jms.ConnectionFactory
import javax.jms.Message
import javax.jms...
```
Becomes:
```java
import jakarta.jms.ConnectionFactory
import jakarta.jms.Message
import jakarta.jms...
```
To use the Jakarta transition client available with ActiveMQ 5.18.x in Maven:
```xml
<dependency>
  <groupId>org.apache.activemq</groupId>
  <artifactId>activemq-client-jakarta</artifactId>
  <version>${activemq.version}</version>
</dependency>
```
Note: With ActiveMQ 6.x the `activemq-client-jakarta` module is removed as it is no longer needed.

### Spring bean usage

Spring bean definitions do not change for Jakarta support.
```xml
<bean class="org.apache.activemq.ActiveMQConnectionFactory">
```
### Implementation Progress

The implementation approach is subject to change. Be sure to verify features in release notes.

User feedback is welcome! Please comment on the JIRAs with questions and comments.

JIRA|Status|Target Version|Completed Version|Feature|Notes
---|:---:|---|---|---|---
[AMQ-7309](https://issues.apache.org/jira/browse/AMQ-7309) | ✅ | 5.16.0 | 5.18.0 | JMS 2.0 API dependency | ActiveMQ will ship with a JMS 2.0 dependency jar
[AMQ-8322](https://issues.apache.org/jira/browse/AMQ-8322) | ✅ | 5.17.0 | 5.18.0 | `JMSContext`, `JMSConsumer`, `JMSProducer`, &amp; `JMSRuntimeException` | Simplified JMS API support
[AMQ-8321](https://issues.apache.org/jira/browse/AMQ-8321) | ✅ | 5.18.0 | 5.18.0 | GetBody/isBodyAssignable | Support for checking body type using a `Class<?>`
[AMQ-8325](https://issues.apache.org/jira/browse/AMQ-8325) | ✅ | 5.18.3, 6.0.0 | 5.18.3, 6.0.0 | XA Connection methods | Updated methods when using XA transactions
[AMQ-8494](https://issues.apache.org/jira/browse/AMQ-8494) | ✅ | 5.17.1 | 5.18.0 | Implement `CLIENT_ACKNOWLEDGEMENT` mode | Client ack requires special handling w/ the simplified JMSContext API
[AMQ-8464](https://issues.apache.org/jira/browse/AMQ-8464) | ✅ | 6.4.0 | 6.4.0 | JMSConsumer | `.receiveBody(Class<T>)` methods
[AMQ-9451](https://issues.apache.org/jira/browse/AMQ-9451) | ✅ | 6.4.0 | 6.4.0 | Pooled `ConnectionFactory` | Support for JMSContext in activemq-jms-pool
[AMQ-8324](https://issues.apache.org/jira/browse/AMQ-8324) | <a href="https://github.com/apache/activemq/pull/1728" title="PR #1543"><i class="fa fa-code-branch" aria-hidden="true"></i>PR-1728</a>| 6.5.0 | | JMSProducer features | `CompletionListener` async send support
[AMQ-8323](https://issues.apache.org/jira/browse/AMQ-8323) | <a href="https://github.com/apache/activemq/pull/2229" title="PR #1543"><i class="fa fa-code-branch" aria-hidden="true"></i>PR-2229</a> | 7.0.0 | | Shared Topic Consumer | Multi-consumer (queue-like) consuming from topic subscriptions
[AMQ-8320](https://issues.apache.org/jira/browse/AMQ-8320) | <a href="https://github.com/apache/activemq/pull/982" title="PR #1543"><i class="fa fa-code-branch" aria-hidden="true"></i>PR-982</a> | 7.0.0  | | Delivery Delay | Support for Message DeliveryDelay feature

### Feature notes

JMS Object|Feature|Notes
---|---|---
`JMSProducer` | disableMessageID | ActiveMQ does not support the optional and rarely used JMS Specification feature to disable JMS message ID

