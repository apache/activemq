---
title: "OSGi Integration"
slug: /osgi-integration
---


Introduction
============

This article will provide more details on how to use ActiveMQ in [Apache Karaf](http://karaf.apache.org/), small OSGi based runtime. Apache Karaf was previously know as _ServiceMix kernel_, so informations found here are applicable to [Apache ServiceMix Enterprise Service Bus](http://servicemix.apache.org/home.html) as well.

> Procedures described in this article were tested using Apache karaf 2.3.0

Installation
============

ActiveMQ provides Karaf features which will help you integrate and use the broker in OSGi environment. For starters you need to add the features URL to Karaf. For version 5.9.0 you can do it like this:
```
karaf@root> features:chooseurl activemq 5.9.0
```
After that you should see newly added features
```
karaf@root> features:list
State         Version         Name                 Repository
\[uninstalled\] \[5.9.0         \] activemq-broker               activemq-5.9.0        
\[uninstalled\] \[5.9.0         \] activemq-http                 activemq-5.9.0        
\[uninstalled\] \[5.9.0         \] activemq-camel                activemq-5.9.0        
\[uninstalled\] \[5.9.0         \] activemq-web-console          activemq-5.9.0
```
Installing and running the broker is as simple as installing `activemq-broker` feature, like
```
karaf@root> features:install activemq-broker
```
This will install and start the full broker (including the web console), just as if you started the standalone distribution.

Broker Configuration
====================

Broker is configured using OSGi Config Admin mechanism and could be easily managed in [Karaf](http://karaf.apache.org/manual/latest-2.3.x/users-guideCommunity/FAQ/configuration). Configuration can be done by modifying `${KARAF_BASE}/etc/org.apache.activemq.server-default.cfg` file or respective config admin property. An example of the file looks like
```
broker-name=amq-broker
data=${karaf.data}/${broker-name}
config=${karaf.base}/etc/activemq.xml
```
Mandatory properties are listed in the following table

Property Name|Property Description
---|---
broker-name|Name of the broker
config|Location of the XML configuration file

You can also use this file to set other properties which will replace placeholders in XML configuration file, as the `${data`} property is used in this example.

Default XML configuration file is located in the `${KARAF_BASE}/etc/activemq.xml` by default.

Web Console
===========

Web Console is installed by default and can be reached at [http://localhost:8181/activemqweb/](http://localhost:8181/activemqweb/)

The configuration for the console is done in a similar fashion to the broker itself. Configuration is located in `${KARAF_BASE}/etc/org.apache.activemq.webconsole.cfg` and by default looks like
```
webconsole.jms.url=tcp://localhost:61616
webconsole.jmx.url=service:jmx:rmi:///jndi/rmi://localhost:1099/karaf-root
webconsole.jmx.user=karaf
webconsole.jmx.password=karaf
```
**Optional:** In order to use the ActiveMQ console with a broker configured with authentication, it is necessary to configure the username/password for JMS connection as well.
```
webconsole.jms.user=system
webconsole.jms.password=manager
```

Commands
========

After these simple steps you have all necessary libraries installed in the container. Also, now you have specific commands on your disposal that you can use to manage your broker:
```
browse                Display selected messages in a specified destination
bstat                 Displays useful broker statistics
list                  Lists all available brokers in the specified JMX context
purge                 Delete selected destination's messages that matches the message selector
query                 Display selected broker component's attributes and statistics
dstat                 Performs a predefined query that displays useful tabular statistics regarding the specified destination type 
```

> Help on commands
> 
> To obtain some detailed help on a given command, you can run:
> 
> ```
> activemq:[command] --help 
> ```

Broker querying
---------------

Several commands are available to query the broker. To address local brokers, you need to use the `--jmxlocal` parameter.

The following command displays available brokers:
```
karaf@root> activemq:list --jmxlocal
BrokerName = mybroker
```
To have more detailed informations, run:
```
karaf@root> activemq:query --jmxlocal
```
It will display informations about the connectors, list of queues, etc...

You can also browse or purge queues using the `activemq:browse` and `activemq:purge` commands.

