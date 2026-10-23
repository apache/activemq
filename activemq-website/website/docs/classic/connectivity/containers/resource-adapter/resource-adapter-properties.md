---
title: "Resource Adapter Properties"
slug: /resource-adapter-properties
---


The ActiveMQ Resource Adapter allows you to configure several properties that:

*   sets the options used for connection used for inbound message delivery
*   sets the default options used for the outbound connection factory objects.

The properties that can be configured are:

Property Name|Required|Default Value|Description
---|---|---|---
ServerUrl|no|tcp://localhost:61616|The URI to connect to the broker on
UserName|no|defaultUser|User name
Password|no|defaultPassword|Password
Clientid|no|null|The JMS client ID to use
UseInboundSession|no|false|
BrokerXmlConfig|no||The ActiveMQ XML config file to use to deploy an embedded broker. E.g. to configure via an XML configuration file then use **xbean:activemq.xml** or to configure via a [Broker Configuration URI](broker-configuration-uri) (to avoid using Spring, XBean and XML) then use **broker:(tcp://localhost:61616)**

#### Performance related settings

Property Name|Required|Default Value|Description
---|---|---|---
AllPrefetchValues|no||Sets all the prefetch values in one setting
DurableTopicPrefetch|no|100|The maximum number of messages sent to a consumer on a durable topic until acknowledgements are received
QueuePrefetch|no|1000|The maximum number of messages sent to a consumer on a queue until acknowledgements are received
InputStreamPrefetch|no|100|The maximum number of messages sent to a consumer on a JMS stream until acknowledgements are received
TopicPrefetch|no|Short.MAX_VALUE - 1|The maximum number of messages sent to a consumer on a non-durable topic until acknowledgements are received

#### Redelivery properties

Property Name|Required|Default Value|Description
---|---|---
InitialRedeliveryDelay|no|1000|The delay before redeliveries start. Also configurable on the ActivationSpec.
MaximumRedeliveries|no|5|The maximum number of redeliveries or -1 for no maximum. Also configurable on the ActivationSpec.
RedeliveryBackOffMultiplier|no|5|The multiplier to use if exponential back off is enabled. Also configurable on the ActivationSpec.
RedeliveryUseExponentialBackOff|no|false|To enable exponential backoff. Also configurable on the ActivationSpec.

#### ServerUrl

The URL to the ActiveMQ server that you want this connection to connect to. If using an embedded broker, this value should be 'vm://localhost' if using the auto-created embedded broker, otherwise if you explicitly create a broker (e.g. using one of the [embedded broker](how-do-i-embed-a-broker-inside-a-connection) techniques), then this value should be 'vm://brokerName', where brokerName is replaced by the broker's name.

#### UserName

The default user name that will be used to establish connections to the ActiveMQ server.

#### Password

The default password that will be used to log the default user into the ActiveMQ server.

#### Clientid

The client id that will be set on the connection that is established to the ActiveMQ server.

#### UseInboundSession

Boolean to configure if outbound connections should reuse the inbound connection's session for sending messages.

#### BrokerXmlConfig

Sets the XML configuration file used to configure the embedded ActiveMQ broker via Spring if using embedded mode. If this property is omitted then no embedded broker is used and you must [run the broker](run-broker) in a separate process or deployment unit.

BrokerXmlConfig is the filename which is assumed to be on the classpath unless a URL is specified. So a value of foo/bar.xml would be assumed to be on the classpath whereas file:dir/file.xml would use the file system. Any valid URL string is supported.

