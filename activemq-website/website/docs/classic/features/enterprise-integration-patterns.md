---
title: "Enterprise Integration Patterns"
slug: /enterprise-integration-patterns
---


Enterprise Integration Patterns
-------------------------------

Version 5.0 onwards of Apache ActiveMQ comes complete with full support for the [Enterprise Integration Patterns](http://www.enterpriseintegrationpatterns.com/toc.html) (from the excellent book by [Gregor Hohpe](http://www.amazon.com/exec/obidos/search-handle-url/105-9796798-8100401?%5Fencoding=UTF8&search-type=ss&index=books&field-author=Gregor%20Hohpe) and [Bobby Woolf](http://www.amazon.com/exec/obidos/search-handle-url/105-9796798-8100401?%5Fencoding=UTF8&search-type=ss&index=books&field-author=Bobby%20Woolf)) via the [Apache Camel library](http://activemq.apache.org/camel/).

You can easily add any of the supported [Enterprise Integration Patterns](enterprise-integration-patterns) into ActiveMQ (either on the JMS client or in the broker process) to support smart routing, transformation and a whole host of other powerful patterns. You can of course just embed [Camel library](http://activemq.apache.org/camel/) directly into your application, such as via [Spring](http://activemq.apache.org/camel/spring.html) as well..

This also means you can cleanly integrate all of the [Camel Components](http://activemq.apache.org/camel/components.html) into ActiveMQ so you can easily integrate with [CXF](http://activemq.apache.org/camel/cxf.html), [Files](http://activemq.apache.org/camel/file.html), [JBI](http://activemq.apache.org/camel/jbi.html), [JPA](http://activemq.apache.org/camel/jpa.html), [Mail](http://activemq.apache.org/camel/mail.html), [MINA](http://activemq.apache.org/camel/mina.html), [Quartz](http://activemq.apache.org/camel/quartz.html), [XMPP](http://activemq.apache.org/camel/xmpp) and [many other protocols and transports!](http://activemq.apache.org/camel/components.html)

### Using EIP in the ActiveMQ Broker

The broker's `activemq.xml` file comes already configured to support Camel; you just need to customize the routing rules.

#### Writing EIP rules using Java code

To use [Java code to write EIP routing rules](http://activemq.apache.org/camel/dsl.html), just put your classes on the classpath (such as in activemq/lib/myroutes/foo.jar). Then to get Camel to find your routes you need to edit the activemq.xml so that the **packages** attribute points to the package name (or a parent package name) to look for.

For example if all your routes are in the package `org.acme.cheese` such as `org.acme.cheese.whatnot.MyRouter` then you could edit the XML to be...
```
<camelContext xmlns="http://activemq.apache.org/camel/schema/spring">
 <package>org.acme.cheese</package>
</camelContext>
```

#### Writing EIP rules using XML

To use XML its even easier, as you can just embed whatever routing rules inside the `<camelContext>` element using Camel's [Spring XML format](http://activemq.apache.org/camel/xml-configuration). Note that the XML is way more verbose than the [Java DSL](http://activemq.apache.org/camel/dsl.html) but it is a bit simpler to deploy. e.g. for a trivial route...
```
<camelContext xmlns="http://activemq.apache.org/camel/schema/spring">
  <route>
    <from uri="activemq:com.acme.MyQueue"/>
    <to uri="activemq:com.acme.SomeOtherQueue"/>
  </route>
</camelContext>
```
### Using EIP in the JMS client

You can use [Camel](http://activemq.apache.org/camel/) [Endpoints](http://activemq.apache.org/camel/endpoint.html) directly from your JMS client one of the following JMS destinations, depending on what JMS API you want it to use

*   [CamelDestination](http://activemq.apache.org/maven/activemq-core/apidocs/org/apache/activemq/camel/CamelDestination.html)
*   [CamelQueue](http://activemq.apache.org/maven/activemq-core/apidocs/org/apache/activemq/camel/CamelQueue.html)
*   [CamelTopic](http://activemq.apache.org/maven/activemq-core/apidocs/org/apache/activemq/camel/CamelTopic.html)

You can then use this destination like any other JMS destination when sending or receiving messages! This means you can use any of the [large number of Camel components](http://activemq.apache.org/camel/components.html) from your JMS code, by just changing the Destination object!

### See Also

*   [Enterprise Integration Patterns](http://activemq.apache.org/camel/enterprise-integration-patterns)
*   [Camel Components](http://activemq.apache.org/camel/components.html)
*   [Camel XML Reference](http://activemq.apache.org/camel/xml-reference)

