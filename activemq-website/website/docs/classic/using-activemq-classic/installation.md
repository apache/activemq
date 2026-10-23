---
title: "Installation"
description: "Legacy ActiveMQ installation steps: download and unpack the binary distribution, run the activemq script from bin, and point it at an XML configuration file."
slug: /installation
displayed_sidebar: docs
---


*   [Download](/components/classic/download) a binary distribution of ActiveMQ and unpack it into some directory.

*   To run an ActiveMQ broker, type the following commands from the directory in which you have just unpacked the ActiveMQ distribution.
```
cd bin
activemq
```
The ActiveMQ broker should now run. You can configure the broker by specifying an [Xml Configuration](xml-configuration) file as a parameter to the _activemq_ command.

You can now run the [Examples](examples) using Ant.

See the [getting started guide](initial-configuration) for details of which jars you need to add to your classpath to start using ActiveMQ in your Java code

If you want to use JNDI to connect to your JMS provider then please view the [JNDI Support](jndi-support). If you are a Spring user you should read about [Spring Support](spring-support)

