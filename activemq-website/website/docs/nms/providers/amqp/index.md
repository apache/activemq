---
title: "Apache.NMS.AMQP"
---

### Overview
The goal of this project is to combine the [.NET Message Service API](/components/nms/documentation) (NMS) with
the [Advanced Message Queuing Protocol (AMQP)](https://www.amqp.org/) 1.0 standard wireline protocol. Historically, the Apache community created the NMS API which provided a vendor agnostic .NET interface to a variety of messaging systems. 

The NMS API gives the flexibility to write .NET applications in C#, VB or any other .NET language, all while using a single API to connect to any number of messaging providers. The Advanced Message Queuing Protocol (AMQP) is an open and standardized internet protocol for reliably passing messages between applications or organizations.

Before AMQP became a standard, organizations used proprietary wireline protocols to connect their systems which lead to vendor lock-in and integration problems when integrating with external organizations.

The key to enabling vendor independence and mass adoption of technology is to combine open source APIs and standard wireline protocols which is precisely what this  project is all about.  Here's how AMQP 1.0 support within NMS helps the .NET community:
 - __More Choice:__ As more message brokers and services implement the AMQP 1.0 standard wireline, .NET developers and architects will have more options for messaging technology.
 - __No Migration Risk:__ Since AMQP 1.0 is a wireline standard, you won't run into the problems that used to happen when switching between implementations.
 - __Innovation:__ Competition is a key component of technology innovation. Directly competitive messaging implementations, with seamless pluggability, forces vendors to innovate and differentiate.

### Download

See here for [downloads](/components/nms/providers/amqp/downloads)

### Configuration

See here for [configuration](/components/nms/providers/amqp/uri-configuration) details


### Contributing
If you are a .NET developer that doesn't want to be locked into a messaging implementation then get engaged with this project. Here you will find the open source code base and please provide comments and make your own enhancements. 

Grab the Source from git
[https://github.com/apache/activemq-nms-amqp](https://github.com/apache/activemq-nms-amqp)

#### Overall Architecture
Apache-NMS-AMQP should bridge the familiar NMS concepts to AMQP protocol concepts as described in the document [amqp-bindmap-jms-v1.0-wd09.pdf](https://www.oasis-open.org/committees/download.php/60574/amqp-bindmap-jms-v1.0-wd09.pdf).
So in general most of the top level classes that implement the Apache.NMS interface _Connection, Session, MessageProducer,_ etc  create, manage, and destroy the amqpnetlite equivalent object _Connection, Session, Link,_ etc.

#### AMQP1.0 Protocol Engine AmqpNetLite
Apache-NMS-AMQP uses [AmqpNetLite](https://github.com/Azure/amqpnetlite) as the underlying AMQP 1.0 transport Protocol engine. 
