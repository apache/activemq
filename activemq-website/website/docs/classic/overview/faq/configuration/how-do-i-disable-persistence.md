---
title: "How do I disable persistence"
slug: /how-do-i-disable-persistence
---

There are three main ways to disable persistence, such as for unit testing JMS code

1.  Set the NON_PERSISTENT message delivery flag on your MessageProducer
2.  Set the `persistent=false` flag in the `<broker/>` element of the [Xml Configuration](xml-configuration) or on the property [BrokerService](http://incubator.apache.org/activemq/maven/activemq-core/apidocs/org/apache/activemq/broker/BrokerService.html)
3.  Delete the persistence files before running your tests (a bit hacky)

If you are unit testing you may be interested in [How To Unit Test JMS Code](how-to-unit-test-jms-code).

Please refer to the [Initial Configuration](initial-configuration) guide on how to disable persistence via system properties, java code or using the [Xml Configuration](xml-configuration)

