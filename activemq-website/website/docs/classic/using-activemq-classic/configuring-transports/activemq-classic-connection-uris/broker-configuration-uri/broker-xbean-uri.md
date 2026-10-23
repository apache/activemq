---
title: "Broker XBean URI"
description: "Start an ActiveMQ Classic broker from an XML configuration with the xbean: URI, pointing at an activemq.xml on the classpath, on the file system or at a URL."
slug: /broker-xbean-uri
---


Broker XBean URI
----------------

The Broker XBean URI allows you to [run a configured broker](run-broker) by referencing an [Xml Configuration](xml-configuration) on the classpath. The URI points to an XML document which can be parsed via XBean or Spring. This URI is typically on the classpath; though in 4.2 onwards you can point to a file or URL as well.

#### Syntax

Syntax|Description
---|---
xbean:classPathResource|Where classPathResource is some name which is resolved on the classpath and interpreted to be an XML document.
xbean:file:filePathResource|Where filePathResource is some name which is resolved on the file system and interpreted to be an XML document.
xbean:urlResource|Where urlResource is some resource which points to an XML document

##### Example URI

The following example starts up a broker using the activemq.xml configuration file which is on the classpath
```
xbean:activemq.xml
```
The following example starts up a broker using the activemq.xml configuration file which is in the current directory
```
xbean:file:./activemq.xml
```