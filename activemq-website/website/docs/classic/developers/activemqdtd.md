---
title: "activemq.dtd"
slug: /activemqdtd
---

We support a custom [Xml Configuration](xml-configuration) mechanism for configuring a Message Broker instance.

To provide tools and IDEs with an easy way of editing the XML as well as providing some handy documentation, we provide our own [activemq.dtd](http://svn.activemq.org/trunk/activemq/modules/core/src/conf/org/activemq/activemq.dtd?view=markup) file which describes all of the various configuration options available.

Even though we're using a DTD we still support custom extension of the XML via the Spring `<bean>` and `<property>` tags.
