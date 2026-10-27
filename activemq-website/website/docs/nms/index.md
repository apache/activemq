---
title: "NMS Documentation"
slug: /documentation
sidebar_position: 1
---

### API Overview

The [NMS API](/components/nms/nms-api) (_.Net Message Service API_) provides a standard .NET interface to Messaging Systems. There are multiple implementations for different [providers](#providers). The NMS API allows you to build .NET applications in C#, VB, or any other .NET language, using a single API to connect to multiple different providers using a JMS style API.

NMS API currently supports all of the features of JMS in a simple pure C# API and implementation apart from XA. Current features include:

*   creating & disposing of connections, sessions, producers, consumers
*   sending of messages to topics, queues with durable or non durable along with temporary destination support
*   synchronous consuming (blocking receive, receive with no wait or receive with a timeout)
*   asynchronous consuming (adding a MessageListener to be dispatched in the thread pool)
*   message header support along with custom properties
*   Text, Bytes, Stream and Map message support
*   support for transactions (sending and acknowledging multiple messages in an atomic transaction)
*   redelivery of messages in rollbacks up to some configured maximum redelivery count
*   Optional Queue browser interface for providers that can support it.
*   Optional .NET Distributed Transaction Support for providers that can support that.

*   [API](/components/nms/nms-api)
*   [API Downloads](/components/nms/nms-api-downloads)
*   [Providers](#providers)
*   [Examples](#examples)
*   [URI Configuration](/components/nms/nms-uri-configuration)

---

### Providers {#providers}

An NMS Provider is a .NET Assembly that provides an implementation of the NMS API that provides connectivity with a particular Messaging Service or an implementation of a standard Messaging Protocol. Currently, the following providers are available:

*   [AMQP](/components/nms/providers/amqp) provides support for AMQP 1.0 protocol. Compatible with ActiveMQ 5.x, ActiveMQ Artemis and any other AMQP 1.0 compliant broker.
*   [ActiveMQ (OpenWire)](/components/nms/providers/activemq) client which communicates with ActiveMQ 5.x using its own native wire protocol and provides many [advanced features](/components/nms/providers/activemq/advanced-features) beyond the standard NMS API.
*   [Other providers](/components/nms/providers/other-providers) Older providers.

---

### Examples {#examples}

This is where you will find examples of how to use the NMS API. These examples demonstrate using the NMS API generically and are limited to features exposed directly through the API model. For examples covering more advanced features of a specific NMS Provider, refer to the documentation for that NMS Provider.

*   [NMS Simple Asynchronous Consumer Example](/components/nms/examples/nms-simple-asynchronous-consumer-example)
*   [NMS Simple Synchronous Consumer Example](/components/nms/examples/nms-simple-synchronous-consumer-example)
