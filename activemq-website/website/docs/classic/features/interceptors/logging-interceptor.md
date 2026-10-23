---
title: "Logging Interceptor"
slug: /logging-interceptor
---

Logging Interceptor
-------------------

The Logging Interceptor is a pretty trivial [Interceptor](./index.md) which just logs to [Jakarta Commons Logging](http://jakarta.apache.org/commons/logging/) or [log4j](http://logging.apache.org/log4j/docs/) as messages are sent or acknowledged on a broker.

The default logging level used is **INFO**. If you want to increase/reduce the logging you can use [change it via commons logging or log4j](how-do-i-change-the-logging).

As of version 5.3 the logging Interceptor is more configurable. The idea is to log all events but be able to turn off logging for certain event groups by configuration.

Currently the following groups are supported:

Attribute|Description|Default Value
---|---|---
logAll|Log all Events|false
logConnectionEvents|Events related to connections and sessions|true
logTransactionEvents|Events related to transaction handling|false
logConsumerEvents|Events related to consuming messages|false
logProducerEvents|Events related to producing messages|false
logInternalEvents|Events normally not of Interest for users like failover, querying internal objects etc|false
logSessionEvents|Events related to adding and removing sessions|true

Only connection events logging is enabled by default.

You can enable the logging interceptor using the following [XML configuration](http://svn.apache.org/repos/asf/activemq/trunk/activemq-unit-tests/src/test/resources/org/apache/activemq/util/plugin-broker.xml). This example enables all logging and disables the connection event logging:
```
<beans 
  xmlns="http://www.springframework.org/schema/beans" 
  xmlns:amq="http://activemq.apache.org/schema/core"
  xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
  xsi:schemaLocation="http://www.springframework.org/schema/beans http://www.springframework.org/schema/beans/spring-beans-2.0.xsd
  http://activemq.apache.org/schema/core http://activemq.apache.org/schema/core/activemq-core.xsd">
  <bean class="org.springframework.beans.factory.config.PropertyPlaceholderConfigurer"/>
  <broker useJmx="false" persistent="false" xmlns="http://activemq.apache.org/schema/core">
    <plugins>
    
      <!-- lets enable detailed logging in the broker but ignore ConnectionEvents -->
      <loggingBrokerPlugin logAll="true" logConnectionEvents="false"/>
      
      <timeStampingBrokerPlugin zeroExpirationOverride="1000" ttlCeiling="60000" futureOnly="true"/>    
      <traceBrokerPathPlugin/>
      
    </plugins>
  </broker>
</beans>
```