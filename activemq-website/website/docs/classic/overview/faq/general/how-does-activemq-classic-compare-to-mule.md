---
title: "How does ActiveMQ compare to Mule"
slug: /how-does-activemq-classic-compare-to-mule
---

Apache ActiveMQ is a messaging provider, with extensive capabilities for message brokering. Mule is described as an ESB, in that it defines and executes the brokering of message exchanges among integrated software components.

#### Architecture

Mule provides an accessible programming model for integration, designed to work well with - and leverage - other middleware technologies. For this reason, Mule does not provide a native messaging system, and it is therefore frequently used with Apache ActiveMQ.

If you use Mule to define an integration based on Apache ActiveMQ, you would use a distinct framework, (the Mule Component Model) to define interfaces for connectivity, as well as mediating activities such as transformations and exception handling. (Note that the Mule framework model presumes a SEDA process model.)Used in this manner, you could think of Mule as a library for defining the interactions among components, with the advantage that it is loosely coupled to both the integrated components and the messaging infrastructure.

If you have made the choice to use Apache ActiveMQ for messaging and such loose coupling to the messaging infrastructure is not required, you can also configure ActiveMQ directly or through the [Apache Camel container](http://activemq.apache.org/camel/). This may give you access to message handling features not defined in the Mule framework, while still providing connectivity to non-JMS clients through ActiveMQ's numerous options for client connectivity. Working with ActiveMQ directly also allows you to take advantage of clustering and failover capabilities that lie outside the scope of the Mule Component Model.

#### Points in Common

The Mule framework is quite user-friendly, is Java-based (Mule components, called Universal Model Objects, are essentially POJOs), and is both flexible (deploys to virtually any Java environment) and lightweight (can be deployed in Spring). These are advantages that it shares with ActiveMQ, so it's not surprising that the two technologies were often used together, before the advent of [Apache Camel](http://activemq.apache.org/camel)

#### "ESB" versus "Messaging"

Mule supports a number of integration capabilities that go beyond the scope of messaging, such as BPEL-based orchestration and (SOAP-to-Java) Web Services support. These are rightly described as ESB features, in that they are more than what one would expect from a messaging platform alone. For environments with these requirements, Camel or Mule provides a good extension to the capabilities of ActiveMQ.

Additional ESB requirements, such as support for a distributed architecture and a native JBI container and components, are not supported by Mule. If you require these capabilities, we encourage you to investigate other open source ESBs that also integrate Apache ActiveMQ, such as [Apache ServiceMix](http://servicemix.org/), and even the [Apache CXF](http://incubator.apache.org/cxf/) service framework.

#### Commercial Support, Copyright and License

Both Mule and ActiveMQ have commercial support available from companies dedicated to the technologies. Commercial support options for ActiveMQ are listed [here](/support).

