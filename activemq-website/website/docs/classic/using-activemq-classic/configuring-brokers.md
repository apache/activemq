---
title: "Configuring Brokers"
description: "Legacy ActiveMQ 4.x guide to the XML broker configuration format, with the activemq-4.0.xsd schema and a journaledJDBC and multicast example."
slug: /configuring-brokers
displayed_sidebar: docs
---


### Overview

In ActiveMQ 4, we are using a new xml format to configure the broker.

### Syntax

The following xsd describes the syntax of the broker configuration file.

activemq-4.0.xsd

And here is the reference documentation for that xsd file.

activemq-4.0.xsd.html

### Example
```
<beans xmlns="http://activemq.org/config/1.0">

  <broker useJmx="true">
  
    <persistenceAdapter>
      <journaledJDBC journalLogFiles="5" dataDirectory="../data"/>
    </persistenceAdapter>
  
    <transportConnectors>
      <transportConnector uri="discovery:tcp://localhost:61616"/>
    </transportConnectors>
    
    <networkConnectors>
      <networkConnector uri="multicast"/>
    </networkConnectors>
    
  </broker>
  
</beans>
```
