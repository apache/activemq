---
title: "Use Cases"
description: "Messaging use cases ActiveMQ Classic targets: transactional XA messaging, market data distribution, clustering, Ajax web streaming and a RESTful HTTP API."
slug: /use-cases
---


Messaging is a diverse and wide ranging subject - there are many different use cases, requirements and deployment options. This is one of the reasons why its so interesting to work on

This document tries to highlight some of the main use cases we are trying to tackle with ActiveQM

Transactional messaging
-----------------------

This use case focusses on transactionality, persistence, never ever missing a message and processing each message exactly once irrespective of system failures.

Typically the JMS connection will be used in an XA way with other JMS connections or other XA resources like JDBC / EJBs etc.

Servers are required for persistence, maybe clusters of servers to increase availability. High availability of servers is an added bonus.

Typically if any node is offline then all the messages will be persisted for when the node comes back online.

High performance market data distribution
-----------------------------------------

This use case focusses on throughput and efficient routing. The idea is to distribute massive volumes of rapidly changing data around a large network as quickly as possible.

Typically throughtput and performance is key - as the amount of data and rate of change is very high and so persistence is rarely used and missing a message is often acceptable in times of failure as old data is often not necessary, the latest prices are what matters.

Clustering and general purpose async messaging model
----------------------------------------------------

This use case focusses on latency and speed.

When implementing web or EJB based clustering the aim is to maintain a cluster of nodes, typically using multicast for discovery & keep-alive and then using direct socket connections to communicate efficiently between buddies. Please see [ActiveCluster](http://activecluster.codehaus.org/) for an example API and implementation of this model of clustering.

This is similar to using a JMS provider as an RMI layer in EJB-style or WS style services - again could end up being mostly multicast for discovery & direct sockets for communication to minimise latency. i.e. rather than having separate servers in between clients, the clients end up talking directly with each other to reduce latency.

Web Streaming of data
---------------------

This use case focusses on [Ajax](ajax) support in ActiveMQ.

Increasingly folks want to stream data real time right into web browsers. For example streaming financial stock prices, to show live IM conversations, live auctions or to dynamically update live content and news.

In this use case we integrate ActiveMQ into a web container and provide close web-integration to enable HTTP POSTs to publish messages and slow JavaScript HTTP GET operations to receive messages.

RESTful API to messaging using HTTP
-----------------------------------

This use case focusses on connectivity and cross-language & cross-technology connectivity.

We can provide a HTTP interface to the message broker allowing a simple cross-language and technology agnostic API to publishing messages or receiving them. To send a message, HTTP POST it to the Message Broker, to receive a message, HTTP GET it. Use the URI and request parameters to denote the destination.

