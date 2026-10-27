---
title: "5.8 Migration Guide"
slug: /58-migration-guide
---

There are some changes in 5.8 that may require some code change

1.  New modules may effect your maven pom dependencies  
    `activemq-core` has been replaced with `activemq-client` and `activemq-broker`. The various message stores have their own activemq-xx-store module. The uber module `activemq-all` remains.
2.  Changed JMX Mbean ObjectNames will effect scripts or tools that directly reference Mbean names  
    All mbeans now share the type=Broker attribute, which gives them containment. In this way, consumers hang off of destinations, which hang off the broker.  
    The different Mbean types are identified by the presence of specific identifiers in their ObjectNames. The mapping from old to new ObjectName is as follows:
    
Type|Old Name|New Name
---|---|---
Broker|Type=Broker|type=Broker
Destination|Type=Queue\|Topic,Destination=<destination identifier>|type=Broker,destinationType=Queue\|Topic,destinationName=<destination identifier>
Connector|Type=Connector|type=Broker,connector=clientConnectors
NetworkConnector|Type=NetworkConnector|type=Broker,connector=networkConnectors
Connection|Type=Connection|type=Broker,connector=*,connectionViewType=remoteAddress\|clientId
		
3.  OSGi integration has changed. The full details are at [OSGi Integration](osgi-integration). In summary:
    1.  There is a single uber OSGI bundle
    2.  The broker and webconsole are now configured via config admin pid files through an OSGi managed service factory.
4.  The ActiveMQ binary file `bin/run.jar` has been renamed to `bin/activemq.jar`.