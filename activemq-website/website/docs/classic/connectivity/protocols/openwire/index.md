---
title: "OpenWire"
slug: /openwire
---


OpenWire is our cross language [Wire Protocol](wire-protocol) to allow native access to ActiveMQ from a number of different languages and platforms. The Java OpenWire transport is the default transport in ActiveMQ 4.x or later. For other languages see the following...

*   [NMS](/components/nms) for the C# API to Messaging and the OpenWire implementation in C#
*   [CMS](cms) for the C++ API to Messaging and the OpenWire implementation in C++

**Note** that you can also use [Stomp](stomp) to access ActiveMQ from many different languages as well as use [GCJ](how-do-i-access-activemq-classic-from-c) or [IKVM](http://activemq.apache.org/nms/) to access the Java code for ActiveMQ from C/C++ or .Net respectively without using OpenWire.

### How it works

OpenWire works by code generating language specific commands and marshaling code which understands the core OpenWire protocol. Then we can write language specific extensions for the low level protocol to provide a nice and easy to use client API in different languages.

To give you an idea, [here](https://github.com/apache/activemq/tree/main/activemq-openwire-generator/src/main/java/org/apache/activemq/openwire/tool/) are the Java classes which code generate the C#, Java, C, and CPP client code.

Since OpenWire automates the creation of language specific [Wire Protocol](wire-protocol) implementations; then its a matter of wrapping the language specific TCP/IP socket code and wrapping the API in some easy to use functions/classes.

### Specification

For a detailed specification of what you will see on the wire:

*   [OpenWire Version 2 Specification](openwire-version-2-specification)

