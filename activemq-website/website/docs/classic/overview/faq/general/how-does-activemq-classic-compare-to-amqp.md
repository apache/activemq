---
title: "How does ActiveMQ compare to AMQP"
slug: /how-does-activemq-classic-compare-to-amqp
---

[AMQP](amqp) stands for the _Advanced Message Queue Protocol_ and is a specification for how messaging clients and brokers can interoperate. AMQP is a specification of a wire-level protocol for client to message broker communication. It is not a messaging system like ActiveMQ, but just a messaging protocol.

ActiveMQ now implements AMQP 1.0!

ActiveMQ also supports other open wire protocols:

*   [OpenWire](openwire), a fast binary format
*   [Stomp](stomp), a simple and easily implemented text based-protocol
*   [MQTT](mqtt), a compact binary format for limited devices on an unreliable network

Through these protocols, ActiveMQ can support clients in C, C++, C#, Ruby, Python, Perl, PHP, Pike etc. AMQP is most similar to OpenWire, because both OpenWire and AMQP are designed for high performance messaging, through a binary (rather than text-based) format. As a text-based format, STOMP is much easier to implement, but gives somewhat slower performance.

