---
title: "How should I package applications using Camel and ActiveMQ"
slug: /how-should-i-package-applications-using-camel-and-activemq-classic
---

How should I package applications using Camel and ActiveMQ
----------------------------------------------------------

So you may wish to use Camel's [Enterprise Integration Patterns](enterprise-integration-patterns) inside the ActiveMQ Broker. In which case the stand alone broker is already packaged to work with Camel out of the box; just add your EIP routing rules to ActiveMQ's [Xml Configuration](xml-configuration) like the example routing rule which ships with ActiveMQ 5.x or later. If you want to include some Java routing rules, then just add your jar to somewhere inside ActiveMQ's lib directory.

If you wish to use ActiveMQ and/or Camel in a standalone application, we recommend you just create a normal Spring application; then add the necessary jars and customise the Spring XML and you're good to go.

### What jars do I need

*   [what jars are required for ActiveMQ](initial-configuration)
*   [what jars are required for Camel](what-jars-do-i-need)

