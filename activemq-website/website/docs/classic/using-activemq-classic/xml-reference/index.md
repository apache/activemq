---
title: "Xml Reference"
description: "Find ActiveMQ Classic XML schema (XSD) references, declare the correct namespace in Spring configs, and fix 5.4 validation errors by ordering elements."
slug: /xml-reference
---

ActiveMQ Xml Reference
-----------------------

This page contains a link to the XML reference guides and XML schema documents for [Xml Configuration](xml-configuration) with ActiveMQ releases

### Released Schemas

Version|Reference|XML Schema|XML Schema namespace
---|---|---|---
ActiveMQ v5.17.2| [Reference](http://activemq.apache.org/schema/core/activemq-core-5.17.2-schema.html)  | [http://activemq.apache.org/schema/core/activemq-core-5.17.2.xsd](http://activemq.apache.org/schema/core/activemq-core-5.17.2.xsd)                                                               |[http://activemq.apache.org/schema/core](http://activemq.apache.org/schema/core)
ActiveMQ v5.16.5|[Reference](http://activemq.apache.org/schema/core/activemq-core-5.16.5-schema.html)|[http://activemq.apache.org/schema/core/activemq-core-5.16.5.xsd](http://activemq.apache.org/schema/core/activemq-core-5.16.5.xsd)|[http://activemq.apache.org/schema/core](http://activemq.apache.org/schema/core)

Browse all versions: [Schema listing](http://activemq.apache.org/schema/core/) 

Using the XSDs in configuration files
-------------------------------------

If you are using XBean to parse the XML configurations, the XML validation is optional so you do not need to specify the XML Schema Locations. However if you are using [Spring](#)'s 2.0 XML handling to parse the XML then you need to refer to the XSD locations in your XML document because Spring uses schema namespace or schema location to resolve namespace handlers. Without correct namespace definition you will get messages like below:
```
Unable to locate NamespaceHandler for namespace [http://activemq.org/config/1.0]
```
You can refer to a specific version of the XSD in your XML as follows
```
<beans xmlns="http://www.springframework.org/schema/beans"
    xmlns:amq="http://activemq.org/config/1.0"
    xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
    xsi:schemaLocation="
        http://www.springframework.org/schema/beans http://www.springframework.org/schema/beans/spring-beans-2.0.xsd
        http://activemq.org/config/1.0 http://activemq.apache.org/schema/core/activemq-core-4.1.2.xsd">
```
or if you prefer you can use a generic XSD without the version number
```
<beans xmlns="http://www.springframework.org/schema/beans"
    xmlns:amq="http://activemq.org/config/1.0"
    xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
    xsi:schemaLocation="
        http://www.springframework.org/schema/beans http://www.springframework.org/schema/beans/spring-beans-2.0.xsd
        http://activemq.org/config/1.0 http://activemq.apache.org/schema/activemq-core.xsd">
```
Your IDE won't be able to fetch this XSD, you'll have to manually associate a specific release; however it does mean that you won't have to upgrade your XML config file with each ActiveMQ release; as it will resolve this XSD using the bundled XSD inside the activemq-core.jar.

### Pay attention to the namespace

If you use ActiveMQ 5.0 or earlier you XML namespace definition should point to [http://activemq.org/config/1.0](http://activemq.org/config/1.0). If you use ActiveMQ 5.1 or later the namespace should point to [http://activemq.apache.org/schema/core](http://activemq.apache.org/schema/core).

`ActiveMQ 5.2 example**
```
<beans xmlns="http://www.springframework.org/schema/beans"
    xmlns:amq="http://activemq.apache.org/schema/core"
    xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
    xsi:schemaLocation="
        http://www.springframework.org/schema/beans
        http://www.springframework.org/schema/beans/spring-beans.xsd
        http://activemq.apache.org/schema/core
        http://activemq.apache.org/schema/core/activemq-core-5.2.0.xsd
    ">
</beans>
```

`ActiveMQ 5.0 example**
```
<beans xmlns="http://www.springframework.org/schema/beans"
    xmlns:amq="http://activemq.org/config/1.0"
    xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
    xsi:schemaLocation="
        http://www.springframework.org/schema/beans
        http://www.springframework.org/schema/beans/spring-beans.xsd
        http://activemq.org/config/1.0
        http://activemq.apache.org/schema/core/activemq-core-5.0.0.xsd
    ">
</beans>
```

Both examples above are almost the same. Only one difference is xml namespace. You can find more information about the namespace change on [activemq-dev topic: ActiveMQ namespace change](http://old.nabble.com/ActiveMQ-namespace-change-td16769650.html).

### Schema Validation

> **Note:** The xml configuration ordering constraint is removed in [5.6](https://issues.apache.org/jira/browse/AMQ-3570), the broker element

#### Alphabetically Ordered XML Elements (5.4 - 5.5.1)

XMLSchema:sequence is replaced with an XMLSchema:all

Note: from version 5.4.2, you can disable validation from the command line by appending a query parameter to the xbean url. This will allow you to reuse your existing xml configuration 'as is' if needed.
```
bin/activemq console xbean:conf/activemq.xml?validate=false
```
Beginning in ActiveMQ 5.4, the XML elements inside the `<broker>` element must be ordered alphabetically. If you look at the XSD, this has always been the case with ActiveMQ. The difference in 5.4 is that the XML configuration is actually being validated against the XML schema.

For example, the following XML configuration did not present a problem in ActiveMQ 5.3.x:

``` 
<?xml version="1.0"?>
<beans xmlns="http://www.springframework.org/schema/beans" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" 
xsi:schemaLocation="http://www.springframework.org/schema/beans http://www.springframework.org/schema/beans/spring-beans-2.0.xsd   
http://activemq.apache.org/schema/core http://activemq.apache.org/schema/core/activemq-core.xsd      
http://camel.apache.org/schema/spring http://camel.apache.org/schema/spring/camel-spring.xsd">
  
  <broker xmlns="http://activemq.apache.org/schema/core" brokerName="brokerB1" destroyApplicationContextOnStop="true">
    
    <plugins>
      <loggingBrokerPlugin/>
    </plugins>
    
    <persistenceAdapter>
      <kahaDB directory="${activemq.base}/data/kahadb" enableJournalDiskSyncs="false" indexWriteBatchSize="10000" indexCacheSize="1000"/>
    </persistenceAdapter>
    
    <destinationPolicy>
      <policyMap>
        <policyEntries>
          <policyEntry queue=">" producerFlowControl="true" memoryLimit="5mb">
            <deadLetterStrategy>
              <individualDeadLetterStrategy queuePrefix="DLQ." useQueueForQueueMessages="true"/>
            </deadLetterStrategy>
          </policyEntry>
        </policyEntries>
      </policyMap>
    </destinationPolicy>
    
    <managementContext>
      <managementContext createConnector="false"/>
    </managementContext>
    
    <systemUsage>
      <systemUsage>
        <memoryUsage>
          <memoryUsage limit="420 mb"/>
        </memoryUsage>
        <storeUsage>
          <storeUsage limit="1 gb"/>
        </storeUsage>
        <tempUsage>
          <tempUsage limit="250 mb"/>
        </tempUsage>
      </systemUsage>
    </systemUsage>
    
    <transportConnectors>
      <transportConnector name="tcp" uri="tcp://0.0.0.0:61616"/>
    </transportConnectors>
    
  </broker>
  
</beans>
```

However, in ActiveMQ 5.4, the XML configuration above would cause an exception similar to the following:
```
Caused by: org.xml.sax.SAXParseException: cvc-complex-type.2.4.a: Invalid content was found starting with element 'persistenceAdapter'. One of '{"http://activemq.apache.org/schema/core":producerSystemUsage, "http://activemq.apache.org/schema/core":proxyConnectors, "http://activemq.apache.org/schema/core":regionBroker, "http://activemq.apache.org/schema/core":services, "http://activemq.apache.org/schema/core":shutdownHooks, "http://activemq.apache.org/schema/core":sslContext, "http://activemq.apache.org/schema/core":systemUsage, "http://activemq.apache.org/schema/core":taskRunnerFactory, "http://activemq.apache.org/schema/core":tempDataStore, "http://activemq.apache.org/schema/core":transportConnectorURIs, "http://activemq.apache.org/schema/core":transportConnectors, WC["http://activemq.apache.org/schema/core"](#)}' is expected.
```
To avoid this XML validation error in ActiveMQ 5.4, simply change the ordering of the XML elements so that they are in alphabetical order. Below is an example of the same XML configuration but in alphabetical order:

``` 
<?xml version="1.0"?>
<beans xmlns="http://www.springframework.org/schema/beans" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" 
xsi:schemaLocation="http://www.springframework.org/schema/beans http://www.springframework.org/schema/beans/spring-beans-2.0.xsd   
http://activemq.apache.org/schema/core http://activemq.apache.org/schema/core/activemq-core.xsd      
http://camel.apache.org/schema/spring http://camel.apache.org/schema/spring/camel-spring.xsd">
  
  <broker xmlns="http://activemq.apache.org/schema/core" brokerName="brokerB1" destroyApplicationContextOnStop="true">
    
    <destinationPolicy>
      <policyMap>
        <policyEntries>
          <policyEntry queue=">" producerFlowControl="true" memoryLimit="5mb">
            <deadLetterStrategy>
              <individualDeadLetterStrategy queuePrefix="DLQ." useQueueForQueueMessages="true"/>
            </deadLetterStrategy>
          </policyEntry>
        </policyEntries>
      </policyMap>
    </destinationPolicy>
    
    <managementContext>
      <managementContext createConnector="false"/>
    </managementContext>

    <persistenceAdapter>
      <kahaDB directory="${activemq.base}/data/kahadb" enableJournalDiskSyncs="false" indexWriteBatchSize="10000" indexCacheSize="1000"/>
    </persistenceAdapter>

    <plugins>
      <loggingBrokerPlugin/>
    </plugins>
    
    <systemUsage>
      <systemUsage>
        <memoryUsage>
          <memoryUsage limit="420 mb"/>
        </memoryUsage>
        <storeUsage>
          <storeUsage limit="1 gb"/>
        </storeUsage>
        <tempUsage>
          <tempUsage limit="250 mb"/>
        </tempUsage>
      </systemUsage>
    </systemUsage>
    
    <transportConnectors>
      <transportConnector name="tcp" uri="tcp://0.0.0.0:61616"/>
    </transportConnectors>
    
  </broker>
  
</beans>
```

Configurations can be easily tested against ActiveMQ 5.4 using the ActiveMQ startup script. Below is an example:
```
$ cd $ACTIVEMQ_HOME  
$ ./bin/activemq console xbean:file:/Users/bsnyder/SpringSource/clients/Finra/activemq-example-unordered.xml  
INFO: Using default configuration  
(you can configure options in one of these file: /etc/default/activemq /Users/bsnyder/.activemqrc)

INFO: Invoke the following command to create a configuration file  
./bin/activemq setup \[ /etc/default/activemq | /Users/bsnyder/.activemqrc \]

INFO: Using java '/System/Library/Frameworks/JavaVM.framework/Home/bin/java'  
INFO: Starting in foreground, this is just for debugging purposes (stop process by pressing CTRL+C)  
Java Runtime: Apple Inc. 1.6.0_20 /System/Library/Frameworks/JavaVM.framework/Versions/1.6.0/Home  
Heap sizes: current=258880k free=253106k max=258880k  
JVM args: -Xms256M -Xmx256M -Dorg.apache.activemq.UseDedicatedTaskRunner=true -Djava.util.logging.config.file=logging.properties -Dcom.sun.management.jmxremote -Dactivemq.classpath=/Users/bsnyder/amq/apache-activemq-5.4.1/conf; -Dactivemq.home=/Users/bsnyder/amq/apache-activemq-5.4.1 -Dactivemq.base=/Users/bsnyder/amq/apache-activemq-5.4.1  
ACTIVEMQ_HOME: /Users/bsnyder/amq/apache-activemq-5.4.1  
ACTIVEMQ_BASE: /Users/bsnyder/amq/apache-activemq-5.4.1  
Loading message broker from: xbean:file:/Users/bsnyder/SpringSource/clients/Finra/activemq-example-unordered.xml  
...
```
