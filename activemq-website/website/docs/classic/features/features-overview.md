---
title: "Features Overview"
slug: /features-overview
---


*   Supports a variety of [Cross Language Clients and Protocols](cross-language-clients) from Java, C, C++, C#, Ruby, Perl, Python, PHP
    *   [OpenWire](openwire) for high performance clients in Java, C, C++, C#
    *   [Stomp](stomp) support so that clients can be written easily in C, Ruby, Perl, Python, PHP, ActionScript/Flash, Smalltalk to talk to ActiveMQ as well as any other popular Message Broker
    *   [AMQP](amqp) v1.0 support
    *   [MQTT](mqtt) v3.1 support allowing for connections in an IoT environment.
*   full support for the [Enterprise Integration Patterns](enterprise-integration-patterns) both in the JMS client and the Message Broker
*   Supports many [advanced features](./index.md) such as [Message Groups](message-groups), [Virtual Destinations](virtual-destinations), [Wildcards](wildcards) and [Composite Destinations](composite-destinations)
*   Fully supports JMS 1.1 and J2EE 1.4 with support for transient, persistent, transactional and XA messaging
*   [Spring Support](spring-support) so that ActiveMQ can be easily embedded into Spring applications and configured using Spring's XML configuration mechanism
*   Tested inside popular J2EE servers such as [TomEE](http://tomee.apache.org/), [Geronimo](http://geronimo.apache.org/), JBoss, GlassFish and WebLogic
    *   Includes [JCA 1.5 resource adaptors](resource-adapter) for inbound & outbound messaging so that ActiveMQ should auto-deploy in any J2EE 1.4 compliant server
*   Supports pluggable [transport protocols](protocols) such as [in-VM](how-do-i-use-activemq-classic-using-in-jvm-messaging), TCP, SSL, NIO, UDP, multicast, JGroups and JXTA transports
*   Supports very fast [persistence](persistence) using JDBC along with a high performance journal
*   Designed for high performance clustering, client-server, peer based communication
*   [REST](rest) API to provide technology agnostic and language neutral web based API to messaging
*   [Ajax](ajax) to support web streaming support to web browsers using pure DHTML, allowing web browsers to be part of the messaging fabric
*   [CXF and Axis Support](axis-and-cxf-support) so that ActiveMQ can be easily dropped into either of these web service stacks to provide reliable messaging
*   Can be used as an in memory JMS provider, ideal for [unit testing JMS](how-to-unit-test-jms-code)

