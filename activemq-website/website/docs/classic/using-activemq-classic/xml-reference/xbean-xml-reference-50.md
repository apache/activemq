---
title: "XBean XML Reference 5.0"
description: "Legacy XBean XML reference for ActiveMQ 5.0 covering broker config elements, from networkConnector and amqPersistenceAdapter to policyEntry."
slug: /xbean-xml-reference-50
---


### Elements By Type

#### The _org.apache.activemq.broker.TransportConnector_ Type Implementations

_\<transportConnector>_

#### The _org.apache.activemq.network.jms.InboundQueueBridge_ Type Implementations

_\<inboundQueueBridge>_

Create an Inbound Queue Bridge

#### The _org.apache.activemq.network.NetworkConnector_ Type Implementations

_\<ldapNetworkConnector>_

class to create dynamic network connectors listed in an directory server using the LDAP v3 protocol as defined in RFC 2251, the entries listed in the directory server must implement the ipHost and ipService objectClasses as defined in RFC 2307.

_\<multicastNetworkConnector>_

A network connector which uses some kind of multicast-like transport that communicates with potentially many remote brokers over a single logical {@link Transport} instance such as when using multicast. This implementation does not depend on multicast at all; any other group based transport could be used.

_\<networkConnector>_

A network connector which uses a discovery agent to detect the remote brokers available and setup a connection to each available remote broker

#### The _org.apache.activemq.broker.BrokerService_ Type Implementations

_\<broker>_

An ActiveMQ Message Broker. It consists of a number of transport connectors, network connectors and a bunch of properties which can be used to configure the broker as its lazily created.

#### The _org.apache.activemq.usage.TempUsage_ Type Implementations

_\<tempUsage>_

Used to keep track of how much of something is being used so that a productive working set usage can be controlled. Main use case is manage memory usage.

#### The _org.apache.activemq.broker.region.policy.PendingSubscriberMessageStoragePolicy_ Type Implementations

_\<fileCursor>_

Pending messages

_\<vmCursor>_

Pending messages held

#### The _org.apache.activemq.usage.MemoryUsage_ Type Implementations

_\<memoryUsage>_

Used to keep track of how much of something is being used so that a productive working set usage can be controlled. Main use case is manage memory usage.

#### The _org.apache.activemq.broker.BrokerPlugin_ Type Implementations

_\<authorizationPlugin>_

An authorization plugin where each operation on a destination is checked against an authorizationMap

_\<connectionDotFilePlugin>_

A [DOT](http://www.graphviz.org/) file creator plugin which creates a DOT file showing the current connections

_\<destinationDotFilePlugin>_

A [DOT](http://www.graphviz.org/) file creator plugin which creates a DOT file showing the current topic & queue hierarchies.

_\<jaasAuthenticationPlugin>_

Provides a JAAS based authentication plugin

_\<jaasCertificateAuthenticationPlugin>_

Provides a JAAS based SSL certificate authentication plugin

_\<loggingBrokerPlugin>_

A simple Broker interceptor which allows you to enable/disable logging.

_\<multicastTraceBrokerPlugin>_

A Broker interceptor which allows you to trace all operations to a Multicast socket.

_\<simpleAuthenticationPlugin>_

Provides a simple authentication plugin

_\<timeStampingBrokerPlugin>_

A Broker interceptor which updates a JMS Client's timestamp on the message with a broker timestamp. Useful when the clocks on client machines are known to not be correct and you can only trust the time set on the broker machines. Enabling this plugin will break JMS compliance since the timestamp that the producer sees on the messages after as send() will be different from the timestamp the consumer will observe when he receives the message. This plugin is not enabled in the default ActiveMQ configuration.

_\<udpTraceBrokerPlugin>_

A Broker interceptor which allows you to trace all operations to a UDP socket.

#### The _org.apache.activemq.store.jdbc.JDBCAdapter_ Type Implementations

_\<axionJDBCAdapter>_

Axion specific Adapter. Axion does not seem to support ALTER statements or sub-selects. This means: - We cannot auto upgrade the schema was we roll out new versions of ActiveMQ - We cannot delete durable sub messages that have be acknowledged by all consumers.

_\<blobJDBCAdapter>_

This JDBCAdapter inserts and extracts BLOB data using the getBlob()/setBlob() operations. This is a little more involved since to insert a blob you have to: 1: insert empty blob. 2: select the blob 3: finally update the blob with data value. The databases/JDBC drivers that use this adapter are:

_\<bytesJDBCAdapter>_

This JDBCAdapter inserts and extracts BLOB data using the setBytes()/getBytes() operations. The databases/JDBC drivers that use this adapter are:

_\<db2JDBCAdapter>_

_\<defaultJDBCAdapter>_

Implements all the default JDBC operations that are used by the JDBCPersistenceAdapter.

sub-classing is encouraged to override the default implementation of methods to account for differences in JDBC Driver implementations.

The JDBCAdapter inserts and extracts BLOB data using the getBytes()/setBytes() operations.

The databases/JDBC drivers that use this adapter are:

_\<imageBasedJDBCAdaptor>_

Provides JDBCAdapter since that uses IMAGE datatype to hold binary data. The databases/JDBC drivers that use this adapter are:

*   Sybase
*   MS SQL

_\<informixJDBCAdapter>_

JDBC Adapter for Informix database. Because Informix database restricts length of composite primary keys, length of _container name_ field and _subscription id_ field must be reduced to 150 characters. Therefore be sure not to use longer names for container name and subscription id than 150 characters.

_\<oracleJDBCAdapter>_

Implements all the default JDBC operations that are used by the JDBCPersistenceAdapter.

Subclassing is encouraged to override the default implementation of methods to account for differences in JDBC Driver implementations.

The JDBCAdapter inserts and extracts BLOB data using the getBytes()/setBytes() operations.

The databases/JDBC drivers that use this adapter are:

_\<streamJDBCAdapter>_

This JDBCAdapter inserts and extracts BLOB data using the setBinaryStream()/getBinaryStream() operations. The databases/JDBC drivers that use this adapter are:

*   Axion

#### The _org.apache.activemq.broker.region.policy.PendingQueueMessageStoragePolicy_ Type Implementations

_\<fileQueueCursor>_

Pending

_\<storeCursor>_

Pending messages

_\<vmQueueCursor>_

Pending messages

#### The _javax.jms.TopicConnectionFactory_ Type Implementations

_\<connectionFactory>_

A [Spring](http://www.springframework.org/) enhanced connection factory which will automatically use the Spring bean name as the clientIDPrefix property so that connections created have client IDs related to your Spring.xml file for easier comprehension from [JMX](jmx).

_\<xaConnectionFactory>_

A [Spring](http://www.springframework.org/) enhanced XA connection factory which will automatically use the Spring bean name as the clientIDPrefix property so that connections created have client IDs related to your Spring.xml file for easier comprehension from [JMX](jmx).

#### The _org.apache.activemq.broker.region.virtual.VirtualDestination_ Type Implementations

_\<compositeQueue>_

Represents a virtual queue which forwards to a number of other destinations.

_\<compositeTopic>_

Represents a virtual topic which forwards to a number of other destinations.

_\<virtualTopic>_

Creates [Virtual Topics](virtual-destinations) using a prefix and postfix. The virtual destination creates a wildcard that is then used to look up all active queue subscriptions which match.

#### The _org.apache.activemq.broker.region.policy.PolicyMap_ Type Implementations

_\<policyMap>_

Represents a destination based configuration of policies so that individual destinations or wildcard hierarchies of destinations can be configured using different policies.

#### The _org.apache.activemq.usage.UsageCapacity_ Type Implementations

_\<defaultUsageCapacity>_

Identify if a limit has been reached

_\<usageCapacity>_

Identify if a limit has been reached

#### The _org.apache.activemq.broker.region.policy.PendingMessageLimitStrategy_ Type Implementations

_\<constantPendingMessageLimitStrategy>_

This PendingMessageLimitStrategy is configured to a constant value for all subscriptions.

_\<prefetchRatePendingMessageLimitStrategy>_

This PendingMessageLimitStrategy sets the maximum pending message limit value to be a multiplier of the prefetch limit of the subscription.

#### The _org.apache.activemq.usage.SystemUsage_ Type Implementations

_\<systemUsage>_

Holder for Usage instances for memory, store and temp files Main use case is manage memory usage.

#### The _javax.jms.Destination_ Type Implementations

_\<queue>_

An ActiveMQ Queue

_\<topic>_

An ActiveMQ Topic

#### The _org.apache.activemq.broker.region.policy.SubscriptionRecoveryPolicy_ Type Implementations

_\<fixedCountSubscriptionRecoveryPolicy>_

This implementation of {@link SubscriptionRecoveryPolicy} will keep a fixed count of last messages.

_\<fixedSizedSubscriptionRecoveryPolicy>_

This implementation of {@link SubscriptionRecoveryPolicy} will keep a fixed amount of memory available in RAM for message history which is evicted in time order.

_\<lastImageSubscriptionRecoveryPolicy>_

This implementation of {@link SubscriptionRecoveryPolicy} will only keep the last message.

_\<noSubscriptionRecoveryPolicy>_

This SubscriptionRecoveryPolicy disable recovery of messages.

_\<queryBasedSubscriptionRecoveryPolicy>_

This implementation of {@link SubscriptionRecoveryPolicy} will perform a user specific query mechanism to load any messages they may have missed.

_\<timedSubscriptionRecoveryPolicy>_

This implementation of {@link SubscriptionRecoveryPolicy} will keep a timed buffer of messages around in memory and use that to recover new subscriptions.

#### The _javax.jms.QueueConnectionFactory_ Type Implementations

_\<connectionFactory>_

A [Spring](http://www.springframework.org/) enhanced connection factory which will automatically use the Spring bean name as the clientIDPrefix property so that connections created have client IDs related to your Spring.xml file for easier comprehension from [JMX](jmx).

_\<xaConnectionFactory>_

A [Spring](http://www.springframework.org/) enhanced XA connection factory which will automatically use the Spring bean name as the clientIDPrefix property so that connections created have client IDs related to your Spring.xml file for easier comprehension from [JMX](jmx).

#### The _org.apache.activemq.broker.jmx.ManagementContext_ Type Implementations

_\<managementContext>_

A Flow provides different dispatch policies within the NMR

#### The _org.apache.activemq.store.jdbc.Statements_ Type Implementations

_\<statements>_

#### The _org.apache.activemq.store.PersistenceAdapterFactory_ Type Implementations

_\<amqPersistenceAdapterFactory>_

An implementation of {@link PersistenceAdapterFactory}

_\<journaledJDBC>_

Creates a default persistence model using the Journal and JDBC

#### The _org.apache.activemq.security.AuthorizationEntry_ Type Implementations

_\<authorizationEntry>_

Represents an entry in a {@link DefaultAuthorizationMap} for assigning different operations (read, write, admin) of user roles to a specific destination or a hierarchical wildcard area of destinations.

_\<tempDestinationAuthorizationEntry>_

Represents an entry in a {@link DefaultAuthorizationMap} for assigning different operations (read, write, admin) of user roles to a temporary destination

#### The _org.apache.activemq.broker.region.policy.PendingDurableSubscriberMessageStoragePolicy_ Type Implementations

_\<fileDurableSubscriberCursor>_

Pending messages for durable subscribers

_\<storeDurableSubscriberCursor>_

Pending messages for a durable

_\<vmDurableCursor>_

Pending

#### The _org.apache.activemq.broker.region.group.MessageGroupMapFactory_ Type Implementations

_\<messageGroupHashBucketFactory>_

A factory to create instances of {@link SimpleMessageGroupMap} when implementing the [Message Groups](message-groups) functionality.

_\<simpleMessageGroupMapFactory>_

A factory to create instances of {@link SimpleMessageGroupMap} when implementing the [Message Groups](message-groups) functionality.

#### The _org.apache.activemq.usage.StoreUsage_ Type Implementations

_\<storeUsage>_

Used to keep track of how much of something is being used so that a productive working set usage can be controlled. Main use case is manage memory usage.

#### The _org.apache.activemq.broker.region.policy.DeadLetterStrategy_ Type Implementations

_\<individualDeadLetterStrategy>_

A {@link DeadLetterStrategy} where each destination has its own individual DLQ using the subject naming hierarchy.

_\<sharedDeadLetterStrategy>_

A default implementation of {@link DeadLetterStrategy} which uses a constant destination.

#### The _javax.jms.Topic_ Type Implementations

_\<topic>_

An ActiveMQ Topic

#### The _org.apache.activemq.ActiveMQPrefetchPolicy_ Type Implementations

_\<prefetchPolicy>_

Defines the prefetch message policies for different types of consumers

#### The _org.apache.activemq.broker.Broker_ Type Implementations

_\<loggingBrokerPlugin>_

A simple Broker interceptor which allows you to enable/disable logging.

_\<multicastTraceBrokerPlugin>_

A Broker interceptor which allows you to trace all operations to a Multicast socket.

_\<timeStampingBrokerPlugin>_

A Broker interceptor which updates a JMS Client's timestamp on the message with a broker timestamp. Useful when the clocks on client machines are known to not be correct and you can only trust the time set on the broker machines. Enabling this plugin will break JMS compliance since the timestamp that the producer sees on the messages after as send() will be different from the timestamp the consumer will observe when he receives the message. This plugin is not enabled in the default ActiveMQ configuration.

_\<udpTraceBrokerPlugin>_

A Broker interceptor which allows you to trace all operations to a UDP socket.

#### The _org.apache.activemq.store.PersistenceAdapter_ Type Implementations

_\<amqPersistenceAdapter>_

An implementation of {@link PersistenceAdapter} designed for use with a {@link Journal} and then check pointing asynchronously on a timeout with some other long term persistent storage.

_\<jdbcPersistenceAdapter>_

A {@link PersistenceAdapter} implementation using JDBC for persistence storage. This persistence adapter will correctly remember prepared XA transactions, but it will not keep track of local transaction commits so that operations performed against the Message store are done as a single uow.

_\<journalPersistenceAdapter>_

An implementation of {@link PersistenceAdapter} designed for use with a {@link Journal} and then check pointing asynchronously on a timeout with some other long term persistent storage.

_\<kahaPersistenceAdapter>_

_\<memoryPersistenceAdapter>_

#### The _org.apache.activemq.broker.region.policy.PolicyEntry_ Type Implementations

_\<policyEntry>_

Represents an entry in a {@link PolicyMap} for assigning policies to a specific destination or a hierarchical wildcard area of destinations.

#### The _org.apache.activemq.network.DemandForwardingBridgeSupport_ Type Implementations

_\<compositeDemandForwardingBridge>_

A demand forwarding bridge which works with multicast style transports where a single Transport could be communicating with multiple remote brokers

_\<demandForwardingBridge>_

Forwards messages from the local broker to the remote broker based on demand.

#### The _org.apache.activemq.network.jms.OutboundQueueBridge_ Type Implementations

_\<outboundQueueBridge>_

Create an Outbound Queue Bridge

#### The _org.apache.activemq.store.jdbc.JDBCPersistenceAdapter_ Type Implementations

_\<jdbcPersistenceAdapter>_

A {@link PersistenceAdapter} implementation using JDBC for persistence storage. This persistence adapter will correctly remember prepared XA transactions, but it will not keep track of local transaction commits so that operations performed against the Message store are done as a single uow.

#### The _org.apache.activemq.command.ActiveMQDestination_ Type Implementations

_\<queue>_

An ActiveMQ Queue

_\<topic>_

An ActiveMQ Topic

#### The _org.apache.activemq.network.jms.InboundTopicBridge_ Type Implementations

_\<inboundTopicBridge>_

Create an Inbound Topic Bridge

#### The _org.apache.activemq.network.jms.JmsConnector_ Type Implementations

_\<jmsQueueConnector>_

A Bridge to other JMS Queue providers

_\<jmsTopicConnector>_

A Bridge to other JMS Topic providers

#### The _org.apache.activemq.RedeliveryPolicy_ Type Implementations

_\<redeliveryPolicy>_

Configuration options used to control how messages are re-delivered when they are rolled back.

#### The _org.apache.activemq.security.AuthorizationMap_ Type Implementations

_\<authorizationMap>_

Represents a destination based configuration of policies so that individual destinations or wildcard hierarchies of destinations can be configured using different policies. Each entry in the map represents the authorization ACLs for each operation.

_\<lDAPAuthorizationMap>_

An {@link AuthorizationMap} which uses LDAP

_\<simpleAuthorizationMap>_

An AuthorizationMap which is configured with individual DestinationMaps for each operation.

#### The _org.apache.activemq.security.TempDestinationAuthorizationEntry_ Type Implementations

_\<tempDestinationAuthorizationEntry>_

Represents an entry in a {@link DefaultAuthorizationMap} for assigning different operations (read, write, admin) of user roles to a temporary destination

#### The _org.apache.activemq.broker.region.policy.DispatchPolicy_ Type Implementations

_\<roundRobinDispatchPolicy>_

Simple dispatch policy that sends a message to every subscription that matches the message.

_\<simpleDispatchPolicy>_

Simple dispatch policy that sends a message to every subscription that matches the message.

_\<strictOrderDispatchPolicy>_

Dispatch policy that causes every subscription to see messages in the same order.

#### The _javax.jms.ConnectionFactory_ Type Implementations

_\<connectionFactory>_

A [Spring](http://www.springframework.org/) enhanced connection factory which will automatically use the Spring bean name as the clientIDPrefix property so that connections created have client IDs related to your Spring.xml file for easier comprehension from [JMX](jmx).

_\<xaConnectionFactory>_

A [Spring](http://www.springframework.org/) enhanced XA connection factory which will automatically use the Spring bean name as the clientIDPrefix property so that connections created have client IDs related to your Spring.xml file for easier comprehension from [JMX](jmx).

#### The _javax.jms.Queue_ Type Implementations

_\<queue>_

An ActiveMQ Queue

#### The _org.apache.activemq.broker.region.policy.MessageEvictionStrategy_ Type Implementations

_\<oldestMessageEvictionStrategy>_

An eviction strategy which evicts the oldest message first (which is the default).

_\<oldestMessageWithLowestPriorityEvictionStrategy>_

An eviction strategy which evicts the oldest message with the lowest priority first.

#### The _org.apache.activemq.Service_ Type Implementations

_\<broker>_

An ActiveMQ Message Broker. It consists of a number of transport connectors, network connectors and a bunch of properties which can be used to configure the broker as its lazily created.

_\<commandAgent>_

An agent which listens to commands on a JMS destination

_\<forwardingBridge>_

Forwards all messages from the local broker to the remote broker.

_\<inboundQueueBridge>_

Create an Inbound Queue Bridge

_\<inboundTopicBridge>_

Create an Inbound Topic Bridge

_\<jmsQueueConnector>_

A Bridge to other JMS Queue providers

_\<jmsTopicConnector>_

A Bridge to other JMS Topic providers

_\<ldapNetworkConnector>_

class to create dynamic network connectors listed in an directory server using the LDAP v3 protocol as defined in RFC 2251, the entries listed in the directory server must implement the ipHost and ipService objectClasses as defined in RFC 2307.

_\<managementContext>_

A Flow provides different dispatch policies within the NMR

_\<masterConnector>_

Connects a Slave Broker to a Master when using [Master Slave](masterslave) for High Availability of messages.

_\<memoryUsage>_

Used to keep track of how much of something is being used so that a productive working set usage can be controlled. Main use case is manage memory usage.

_\<multicastNetworkConnector>_

A network connector which uses some kind of multicast-like transport that communicates with potentially many remote brokers over a single logical {@link Transport} instance such as when using multicast. This implementation does not depend on multicast at all; any other group based transport could be used.

_\<networkConnector>_

A network connector which uses a discovery agent to detect the remote brokers available and setup a connection to each available remote broker

_\<outboundQueueBridge>_

Create an Outbound Queue Bridge

_\<outboundTopicBridge>_

Create an Outbound Topic Bridge

_\<proxyConnector>_

_\<storeUsage>_

Used to keep track of how much of something is being used so that a productive working set usage can be controlled. Main use case is manage memory usage.

_\<systemUsage>_

Holder for Usage instances for memory, store and temp files Main use case is manage memory usage.

_\<tempUsage>_

Used to keep track of how much of something is being used so that a productive working set usage can be controlled. Main use case is manage memory usage.

#### The _org.apache.activemq.network.jms.JmsMesageConvertor_ Type Implementations

_\<simpleJmsMessageConvertor>_

Converts Message from one JMS to another

#### The _org.apache.activemq.network.jms.OutboundTopicBridge_ Type Implementations

_\<outboundTopicBridge>_

Create an Outbound Topic Bridge

#### The _org.apache.activemq.broker.region.DestinationInterceptor_ Type Implementations

_\<mirroredQueue>_

Creates [Mirrored Queue](mirrored-queues) using a prefix and postfix to define the topic name on which to mirror the queue to.

_\<virtualDestinationInterceptor>_

Implements [Virtual Topics](virtual-destinations).

#### The _org.apache.activemq.filter.DestinationMap_ Type Implementations

_\<authorizationMap>_

Represents a destination based configuration of policies so that individual destinations or wildcard hierarchies of destinations can be configured using different policies. Each entry in the map represents the authorization ACLs for each operation.

_\<policyMap>_

Represents a destination based configuration of policies so that individual destinations or wildcard hierarchies of destinations can be configured using different policies.

#### The _org.apache.activemq.network.NetworkBridgeConfiguration_ Type Implementations

_\<ldapNetworkConnector>_

class to create dynamic network connectors listed in an directory server using the LDAP v3 protocol as defined in RFC 2251, the entries listed in the directory server must implement the ipHost and ipService objectClasses as defined in RFC 2307.

_\<multicastNetworkConnector>_

A network connector which uses some kind of multicast-like transport that communicates with potentially many remote brokers over a single logical {@link Transport} instance such as when using multicast. This implementation does not depend on multicast at all; any other group based transport could be used.

_\<networkConnector>_

A network connector which uses a discovery agent to detect the remote brokers available and setup a connection to each available remote broker

### The _\<amqPersistenceAdapter>_ Element

An implementation of {@link PersistenceAdapter} designed for use with a {@link Journal} and then check pointing asynchronously on a timeout with some other long term persistent storage.

#### Properties

Property Name

Type

Description

archiveDataLogs

_boolean_

asyncDataManager

_org.apache.activemq.kaha.impl.async.AsyncDataManager_

brokerName

_java.lang.String_

brokerService

_org.apache.activemq.broker.BrokerService_

checkpointInterval

_long_

cleanupInterval

_long_

directory

_java.io.File_

directoryArchive

_java.io.File_

indexBinSize

_int_

indexKeySize

_int_

indexPageSize

_int_

When set using XBean, you can use values such as: "20 mb", "1024 kb", or "1 gb"

maxCheckpointMessageAddSize

_int_

When set using XBean, you can use values such as: "20 mb", "1024 kb", or "1 gb"

maxFileLength

_int_

When set using XBean, you can use values such as: "20 mb", "1024 kb", or "1 gb"

persistentIndex

_boolean_

referenceStoreAdapter

_org.apache.activemq.store.ReferenceStoreAdapter_

syncOnWrite

_boolean_

taskRunnerFactory

_org.apache.activemq.thread.TaskRunnerFactory_

usageManager

_org.apache.activemq.usage.SystemUsage_

useNio

_boolean_

wireFormat

_org.apache.activemq.wireformat.WireFormat_

### The _\<amqPersistenceAdapterFactory>_ Element

An implementation of {@link PersistenceAdapterFactory}

#### Properties

Property Name

Type

Description

brokerName

_java.lang.String_

dataDirectory

_java.io.File_

journalThreadPriority

_int_

maxFileLength

_int_

persistentIndex

_boolean_

referenceStoreAdapter

_org.apache.activemq.store.ReferenceStoreAdapter_

syncOnWrite

_boolean_

taskRunnerFactory

_org.apache.activemq.thread.TaskRunnerFactory_

useNio

_boolean_

### The _\<authenticationUser>_ Element

A helper object used to configure simple authentiaction plugin

#### Properties

Property Name

Type

Description

groups

_java.lang.String_

password

_java.lang.String_

username

_java.lang.String_

### The _\<authorizationEntry>_ Element

Represents an entry in a {@link DefaultAuthorizationMap} for assigning different operations (read, write, admin) of user roles to a specific destination or a hierarchical wildcard area of destinations.

#### Properties

Property Name

Type

Description

admin

_java.lang.String_

adminACLs

(_java.lang.Object_)*

destination

_org.apache.activemq.command.ActiveMQDestination_

groupClass

_java.lang.String_

queue

_java.lang.String_

A helper method to set the destination from a configuration file

read

_java.lang.String_

readACLs

(_java.lang.Object_)*

topic

_java.lang.String_

A helper method to set the destination from a configuration file

write

_java.lang.String_

writeACLs

(_java.lang.Object_)*

### The _\<authorizationMap>_ Element

Represents a destination based configuration of policies so that individual destinations or wildcard hierarchies of destinations can be configured using different policies. Each entry in the map represents the authorization ACLs for each operation.

#### Properties

Property Name

Type

Description

authorizationEntries

(_java.lang.Object_)*

Sets the individual entries on the authorization map

defaultEntry

_org.apache.activemq.security.AuthorizationEntry_

entries

(_java.lang.Object_)*

A helper method to allow the destination map to be populated from a dependency injection framework such as Spring

tempDestinationAuthorizationEntry

_org.apache.activemq.security.TempDestinationAuthorizationEntry_

### The _\<authorizationPlugin>_ Element

An authorization plugin where each operation on a destination is checked against an authorizationMap

#### Properties

Property Name

Type

Description

map

_org.apache.activemq.security.AuthorizationMap_

### The _\<axionJDBCAdapter>_ Element

Axion specific Adapter. Axion does not seem to support ALTER statements or sub-selects. This means: - We cannot auto upgrade the schema was we roll out new versions of ActiveMQ - We cannot delete durable sub messages that have be acknowledged by all consumers.

#### Properties

Property Name

Type

Description

batchStatments

_boolean_

statements

_org.apache.activemq.store.jdbc.Statements_

useExternalMessageReferences

_boolean_

### The _\<blobJDBCAdapter>_ Element

This JDBCAdapter inserts and extracts BLOB data using the getBlob()/setBlob() operations. This is a little more involved since to insert a blob you have to: 1: insert empty blob. 2: select the blob 3: finally update the blob with data value. The databases/JDBC drivers that use this adapter are:

#### Properties

Property Name

Type

Description

batchStatments

_boolean_

statements

_org.apache.activemq.store.jdbc.Statements_

useExternalMessageReferences

_boolean_

### The _\<broker>_ Element

An ActiveMQ Message Broker. It consists of a number of transport connectors, network connectors and a bunch of properties which can be used to configure the broker as its lazily created.

#### Properties

Property Name

Type

Description

adminView

_org.apache.activemq.broker.jmx.BrokerView_

Returns the administration view of the broker; used to create and destroy resources such as queues and topics. Note this method returns null if JMX is disabled.

advisorySupport

_boolean_

Allows the support of advisory messages to be disabled for performance reasons.

brokerName

_java.lang.String_

Sets the name of this broker; which must be unique in the network

brokerObjectName

_javax.management.ObjectName_

Sets the JMX ObjectName for this broker

clustered

_boolean_

consumerSystemUsage

_org.apache.activemq.usage.SystemUsage_

dataDirectory

_java.lang.String_

Sets the directory in which the data files will be stored by default for the JDBC and Journal persistence adaptors.

dataDirectoryFile

_java.io.File_

Sets the directory in which the data files will be stored by default for the JDBC and Journal persistence adaptors.

deleteAllMessagesOnStartup

_boolean_

Sets whether or not all messages are deleted on startup - mostly only useful for testing.

destinationFactory

_org.apache.activemq.broker.region.DestinationFactory_

destinationInterceptors

(_org.apache.activemq.broker.region.DestinationInterceptor_)*

Sets the destination interceptors to use

destinationPolicy

_org.apache.activemq.broker.region.policy.PolicyMap_

Sets the destination specific policies available either for exact destinations or for wildcard areas of destinations.

destinations

(_org.apache.activemq.command.ActiveMQDestination_)*

Sets the destinations which should be loaded/created on startup

enableStatistics

_boolean_

Sets whether or not the Broker's services enable statistics or not.

jmsBridgeConnectors

(_org.apache.activemq.network.jms.JmsConnector_)*

keepDurableSubsActive

_boolean_

managementContext

_org.apache.activemq.broker.jmx.ManagementContext_

masterConnectorURI

_java.lang.String_

messageAuthorizationPolicy

_org.apache.activemq.security.MessageAuthorizationPolicy_

Sets the policy used to decide if the current connection is authorized to consume a given message

networkConnectorURIs

(_java.lang.String_)*

networkConnectors

(_org.apache.activemq.network.NetworkConnector_)*

Sets the network connectors which this broker will use to connect to other brokers in a federated network

persistenceAdapter

_org.apache.activemq.store.PersistenceAdapter_

Sets the persistence adaptor implementation to use for this broker

persistenceFactory

_org.apache.activemq.store.PersistenceAdapterFactory_

persistenceTaskRunnerFactory

_org.apache.activemq.thread.TaskRunnerFactory_

persistenceThreadPriority

_int_

persistent

_boolean_

Sets whether or not persistence is enabled or disabled.

plugins

(_org.apache.activemq.broker.BrokerPlugin_)*

Sets a number of broker plugins to install such as for security authentication or authorization

populateJMSXUserID

_boolean_

Sets whether or not the broker should populate the JMSXUserID header.

producerSystemUsage

_org.apache.activemq.usage.SystemUsage_

proxyConnectors

(_java.lang.Object_)*

Sets the network connectors which this broker will use to connect to other brokers in a federated network

services

(_org.apache.activemq.Service_)*

Sets the services associated with this broker such as a {@link MasterConnector}

shutdownOnMasterFailure

_boolean_

start

_boolean_

Sets whether or not the broker is started along with the ApplicationContext it is defined within. Normally you would want the broker to start up along with the ApplicationContext but sometimes when working with JUnit tests you may wish to start and stop the broker explicitly yourself.

supportFailOver

_boolean_

systemUsage

_org.apache.activemq.usage.SystemUsage_

taskRunnerFactory

_org.apache.activemq.thread.TaskRunnerFactory_

tempDataStore

_org.apache.activemq.kaha.Store_

tmpDataDirectory

_java.io.File_

transportConnectorURIs

(_java.lang.String_)*

transportConnectors

(_org.apache.activemq.broker.TransportConnector_)*

Sets the transport connectors which this broker will listen on for new clients

useJmx

_boolean_

Sets whether or not the Broker's services should be exposed into JMX or not.

useLocalHostBrokerName

_boolean_

useLoggingForShutdownErrors

_boolean_

Sets whether or not we should use commons-logging when reporting errors when shutting down the broker

useMirroredQueues

_boolean_

Sets whether or not [Mirrored Queues](mirrored-queues) should be supported by default if they have not been explicitly configured.

useShutdownHook

_boolean_

Sets whether or not we should use a shutdown handler to close down the broker cleanly if the JVM is terminated. It is recommended you leave this enabled.

useVirtualTopics

_boolean_

Sets whether or not [Virtual Topics](virtual-destinations) should be supported by default if they have not been explicitly configured.

vmConnectorURI

_java.net.URI_

maxInflatedDataSize

_integer_

The maximum allowed size of an uncompressed message body. The default is 100 MB.

### The _\<bytesJDBCAdapter>_ Element

This JDBCAdapter inserts and extracts BLOB data using the setBytes()/getBytes() operations. The databases/JDBC drivers that use this adapter are:

#### Properties

Property Name

Type

Description

batchStatments

_boolean_

statements

_org.apache.activemq.store.jdbc.Statements_

useExternalMessageReferences

_boolean_

### The _\<commandAgent>_ Element

An agent which listens to commands on a JMS destination

#### Properties

Property Name

Type

Description

brokerUrl

_java.lang.String_

commandDestination

_javax.jms.Destination_

connection

_javax.jms.Connection_

connectionFactory

_javax.jms.ConnectionFactory_

### The _\<compositeDemandForwardingBridge>_ Element

A demand forwarding bridge which works with multicast style transports where a single Transport could be communicating with multiple remote brokers

#### Properties

Property Name

Type

Description

configuration

_org.apache.activemq.network.NetworkBridgeConfiguration_

createdByDuplex

_boolean_

durableDestinations

(_org.apache.activemq.command.ActiveMQDestination_)*

dynamicallyIncludedDestinations

(_org.apache.activemq.command.ActiveMQDestination_)*

excludedDestinations

(_org.apache.activemq.command.ActiveMQDestination_)*

localBroker

_org.apache.activemq.transport.Transport_

networkBridgeListener

_org.apache.activemq.network.NetworkBridgeListener_

remoteBroker

_org.apache.activemq.transport.Transport_

staticallyIncludedDestinations

(_org.apache.activemq.command.ActiveMQDestination_)*

### The _\<compositeQueue>_ Element

Represents a virtual queue which forwards to a number of other destinations.

#### Properties

Property Name

Type

Description

copyMessage

_boolean_

Sets whether a copy of the message will be sent to each destination. Defaults to true so that the forward destination is set as the destination of the message

forwardOnly

_boolean_

Sets if the virtual destination is forward only (and so there is no physical queue to match the virtual queue) or if there is also a physical queue with the same name).

forwardTo

(_java.lang.Object_)*

Sets the list of destinations to forward to

name

_java.lang.String_

Sets the name of this composite destination

### The _\<compositeTopic>_ Element

Represents a virtual topic which forwards to a number of other destinations.

#### Properties

Property Name

Type

Description

copyMessage

_boolean_

Sets whether a copy of the message will be sent to each destination. Defaults to true so that the forward destination is set as the destination of the message

forwardOnly

_boolean_

Sets if the virtual destination is forward only (and so there is no physical queue to match the virtual queue) or if there is also a physical queue with the same name).

forwardTo

(_java.lang.Object_)*

Sets the list of destinations to forward to

name

_java.lang.String_

Sets the name of this composite destination

### The _\<connectionDotFilePlugin>_ Element

A [DOT](http://www.graphviz.org/) file creator plugin which creates a DOT file showing the current connections

#### Properties

Property Name

Type

Description

file

_java.lang.String_

Sets the destination file name to create the destination diagram

### The _\<connectionFactory>_ Element

A [Spring](http://www.springframework.org/) enhanced connection factory which will automatically use the Spring bean name as the clientIDPrefix property so that connections created have client IDs related to your Spring.xml file for easier comprehension from [JMX](jmx).

#### Properties

Property Name

Type

Description

alwaysSessionAsync

_boolean_

If this flag is set then a separate thread is not used for dispatching messages for each Session in the Connection. However, a separate thread is always used if there is more than one session, or the session isn't in auto acknowledge or duplicates ok mode

alwaysSyncSend

_boolean_

Set true if always require messages to be sync sent

beanName

_java.lang.String_

blobTransferPolicy

_org.apache.activemq.blob.BlobTransferPolicy_

Sets the policy used to describe how out-of-band BLOBs (Binary Large OBjects) are transferred from producers to brokers to consumers

brokerURL

_java.lang.String_

Sets the [connection URL](configuring-transports) used to connect to the ActiveMQ broker.

clientID

_java.lang.String_

Sets the JMS clientID to use for the created connection. Note that this can only be used by one connection at once so generally its a better idea to set the clientID on a Connection

clientIDPrefix

_java.lang.String_

Sets the prefix used by autogenerated JMS Client ID values which are used if the JMS client does not explicitly specify on.

clientIdGenerator

_org.apache.activemq.util.IdGenerator_

closeTimeout

_int_

Sets the timeout before a close is considered complete. Normally a close() on a connection waits for confirmation from the broker; this allows that operation to timeout to save the client hanging if there is no broker

copyMessageOnSend

_boolean_

Should a JMS message be copied to a new JMS Message object as part of the send() method in JMS. This is enabled by default to be compliant with the JMS specification. You can disable it if you do not mutate JMS messages after they are sent for a performance boost

disableTimeStampsByDefault

_boolean_

Sets whether or not timestamps on messages should be disabled or not. If you disable them it adds a small performance boost.

dispatchAsync

_boolean_

Enables or disables the default setting of whether or not consumers have their messages [dispatched synchronously or asynchronously by the broker](consumer-dispatch-async). For non-durable topics for example we typically dispatch synchronously by default to minimize context switches which boost performance. However sometimes its better to go slower to ensure that a single blocked consumer socket does not block delivery to other consumers.

exclusiveConsumer

_boolean_

Enables or disables whether or not queue consumers should be exclusive or not for example to preserve ordering when not using [Message Groups](message-groups)

nestedMapAndListEnabled

_boolean_

Enables/disables whether or not Message properties and MapMessage entries support [Nested Structures](structured-message-properties-and-mapmessages) of Map and List objects

objectMessageSerializationDefered

_boolean_

When an object is set on an ObjectMessage, the JMS spec requires the object to be serialized by that set method. Enabling this flag causes the object to not get serialized. The object may subsequently get serialized if the message needs to be sent over a socket or stored to disk.

optimizeAcknowledge

_boolean_

optimizedMessageDispatch

_boolean_

If this flag is set then an larger prefetch limit is used - only applicable for durable topic subscribers.

password

_java.lang.String_

Sets the JMS password used for connections created from this factory

prefetchPolicy

_org.apache.activemq.ActiveMQPrefetchPolicy_

Sets the [prefetch policy](what-is-the-prefetch-limit-for) for consumers created by this connection.

producerWindowSize

_int_

properties

_java.util.Properties_

Get the properties from this instance for storing in JNDI

redeliveryPolicy

_org.apache.activemq.RedeliveryPolicy_

Sets the global redelivery policy to be used when a message is delivered but the session is rolled back

statsEnabled

_boolean_

transformer

_org.apache.activemq.MessageTransformer_

Sets the transformer used to transform messages before they are sent on to the JMS bus or when they are received from the bus but before they are delivered to the JMS client

transportListener

_org.apache.activemq.transport.TransportListener_

Allows a listener to be configured on the ConnectionFactory so that when this factory is used with frameworks which don't expose the Connection such as Spring JmsTemplate, you can still register a transport listener.

useAsyncSend

_boolean_

Forces the use of [Async Sends](async-sends) which adds a massive performance boost; but means that the send() method will return immediately whether the message has been sent or not which could lead to message loss.

useBeanNameAsClientIdPrefix

_boolean_

useCompression

_boolean_

Enables the use of compression of the message bodies

useRetroactiveConsumer

_boolean_

Sets whether or not retroactive consumers are enabled. Retroactive consumers allow non-durable topic subscribers to receive old messages that were published before the non-durable subscriber started.

userName

_java.lang.String_

Sets the JMS userName used by connections created by this factory

warnAboutUnstartedConnectionTimeout

_long_

Enables the timeout from a connection creation to when a warning is generated if the connection is not properly started via {@link Connection#start()} and a message is received by a consumer. It is a very common gotcha to forget to [start the connection](i-am-not-receiving-any-messages-what-is-wrong) so this option makes the default case to create a warning if the user forgets. To disable the warning just set the value to < 0 (say -1).

watchTopicAdvisories

_boolean_

### The _\<constantPendingMessageLimitStrategy>_ Element

This PendingMessageLimitStrategy is configured to a constant value for all subscriptions.

#### Properties

Property Name

Type

Description

limit

_int_

### The _\<db2JDBCAdapter>_ Element

#### Properties

Property Name

Type

Description

batchStatments

_boolean_

statements

_org.apache.activemq.store.jdbc.Statements_

useExternalMessageReferences

_boolean_

### The _\<defaultJDBCAdapter>_ Element

Implements all the default JDBC operations that are used by the JDBCPersistenceAdapter.

sub-classing is encouraged to override the default implementation of methods to account for differences in JDBC Driver implementations.

The JDBCAdapter inserts and extracts BLOB data using the getBytes()/setBytes() operations.

The databases/JDBC drivers that use this adapter are:

#### Properties

Property Name

Type

Description

batchStatments

_boolean_

statements

_org.apache.activemq.store.jdbc.Statements_

useExternalMessageReferences

_boolean_

### The _\<defaultUsageCapacity>_ Element

Identify if a limit has been reached

#### Properties

Property Name

Type

Description

limit

_long_

### The _\<demandForwardingBridge>_ Element

Forwards messages from the local broker to the remote broker based on demand.

#### Properties

Property Name

Type

Description

configuration

_org.apache.activemq.network.NetworkBridgeConfiguration_

createdByDuplex

_boolean_

durableDestinations

(_org.apache.activemq.command.ActiveMQDestination_)*

dynamicallyIncludedDestinations

(_org.apache.activemq.command.ActiveMQDestination_)*

excludedDestinations

(_org.apache.activemq.command.ActiveMQDestination_)*

localBroker

_org.apache.activemq.transport.Transport_

networkBridgeListener

_org.apache.activemq.network.NetworkBridgeListener_

remoteBroker

_org.apache.activemq.transport.Transport_

staticallyIncludedDestinations

(_org.apache.activemq.command.ActiveMQDestination_)*

### The _\<destinationDotFilePlugin>_ Element

A [DOT](http://www.graphviz.org/) file creator plugin which creates a DOT file showing the current topic & queue hierarchies.

#### Properties

Property Name

Type

Description

file

_java.lang.String_

Sets the destination file name to create the destination diagram

### The _\<destinationEntry>_ Element

A default entry in a DestinationMap which holds a single value.

#### Properties

Property Name

Type

Description

destination

_org.apache.activemq.command.ActiveMQDestination_

queue

_java.lang.String_

A helper method to set the destination from a configuration file

topic

_java.lang.String_

A helper method to set the destination from a configuration file

value

_java.lang.Object_

### The _\<fileCursor>_ Element

Pending messages

### The _\<fileDurableSubscriberCursor>_ Element

Pending messages for durable subscribers

### The _\<fileQueueCursor>_ Element

Pending

### The _\<filteredDestination>_ Element

Represents a destination which is filtered using some predicate such as a selector so that messages are only dispatched to the destination if they match the filter.

#### Properties

Property Name

Type

Description

destination

_org.apache.activemq.command.ActiveMQDestination_

The destination to send messages to if they match the filter

filter

_org.apache.activemq.filter.BooleanExpression_

queue

_java.lang.String_

Sets the destination property to the given queue name

selector

_java.lang.String_

Sets the JMS selector used to filter messages before forwarding them to this destination

topic

_java.lang.String_

Sets the destination property to the given topic name

### The _\<fixedCountSubscriptionRecoveryPolicy>_ Element

This implementation of {@link SubscriptionRecoveryPolicy} will keep a fixed count of last messages.

#### Properties

Property Name

Type

Description

maximumSize

_int_

Sets the maximum number of messages that this destination will hold around in RAM

### The _\<fixedSizedSubscriptionRecoveryPolicy>_ Element

This implementation of {@link SubscriptionRecoveryPolicy} will keep a fixed amount of memory available in RAM for message history which is evicted in time order.

#### Properties

Property Name

Type

Description

buffer

_org.apache.activemq.memory.list.MessageList_

maximumSize

_int_

Sets the maximum amount of RAM in bytes that this buffer can hold in RAM

useSharedBuffer

_boolean_

### The _\<forwardingBridge>_ Element

Forwards all messages from the local broker to the remote broker.

#### Properties

Property Name

Type

Description

clientId

_java.lang.String_

destinationFilter

_java.lang.String_

dispatchAsync

_boolean_

localBroker

_org.apache.activemq.transport.Transport_

networkBridgeFailedListener

_org.apache.activemq.network.NetworkBridgeListener_

prefetchSize

_int_

remoteBroker

_org.apache.activemq.transport.Transport_

### The _\<imageBasedJDBCAdaptor>_ Element

Provides JDBCAdapter since that uses IMAGE datatype to hold binary data. The databases/JDBC drivers that use this adapter are:

*   Sybase
*   MS SQL

#### Properties

Property Name

Type

Description

batchStatments

_boolean_

statements

_org.apache.activemq.store.jdbc.Statements_

useExternalMessageReferences

_boolean_

### The _\<inboundQueueBridge>_ Element

Create an Inbound Queue Bridge

#### Properties

Property Name

Type

Description

consumer

_javax.jms.MessageConsumer_

consumerConnection

_javax.jms.QueueConnection_

consumerQueue

_javax.jms.Queue_

doHandleReplyTo

_boolean_

inboundQueueName

_java.lang.String_

jmsConnector

_org.apache.activemq.network.jms.JmsConnector_

jmsMessageConvertor

_org.apache.activemq.network.jms.JmsMesageConvertor_

localQueueName

_java.lang.String_

maximumRetries

_int_

Sets the maximum number of retries if a send fails before closing the bridge

producerConnection

_javax.jms.QueueConnection_

producerQueue

_javax.jms.Queue_

selector

_java.lang.String_

### The _\<inboundTopicBridge>_ Element

Create an Inbound Topic Bridge

#### Properties

Property Name

Type

Description

consumer

_javax.jms.MessageConsumer_

consumerConnection

_javax.jms.TopicConnection_

consumerName

_java.lang.String_

consumerTopic

_javax.jms.Topic_

doHandleReplyTo

_boolean_

inboundTopicName

_java.lang.String_

jmsConnector

_org.apache.activemq.network.jms.JmsConnector_

jmsMessageConvertor

_org.apache.activemq.network.jms.JmsMesageConvertor_

localTopicName

_java.lang.String_

maximumRetries

_int_

Sets the maximum number of retries if a send fails before closing the bridge

producerConnection

_javax.jms.TopicConnection_

producerTopic

_javax.jms.Topic_

selector

_java.lang.String_

### The _\<individualDeadLetterStrategy>_ Element

A {@link DeadLetterStrategy} where each destination has its own individual DLQ using the subject naming hierarchy.

#### Properties

Property Name

Type

Description

processExpired

_boolean_

processNonPersistent

_boolean_

queuePrefix

_java.lang.String_

Sets the prefix to use for all dead letter queues for queue messages

topicPrefix

_java.lang.String_

Sets the prefix to use for all dead letter queues for topic messages

useQueueForQueueMessages

_boolean_

Sets whether a queue or topic should be used for queue messages sent to a DLQ. The default is to use a Queue

useQueueForTopicMessages

_boolean_

Sets whether a queue or topic should be used for topic messages sent to a DLQ. The default is to use a Queue

### The _\<informixJDBCAdapter>_ Element

JDBC Adapter for Informix database. Because Informix database restricts length of composite primary keys, length of _container name_ field and _subscription id_ field must be reduced to 150 characters. Therefore be sure not to use longer names for container name and subscription id than 150 characters.

#### Properties

Property Name

Type

Description

batchStatments

_boolean_

statements

_org.apache.activemq.store.jdbc.Statements_

useExternalMessageReferences

_boolean_

### The _\<jaasAuthenticationPlugin>_ Element

Provides a JAAS based authentication plugin

#### Properties

Property Name

Type

Description

configuration

_java.lang.String_

Sets the JAAS configuration domain name used

discoverLoginConfig

_boolean_

Enables or disables the auto-discovery of the login.config file for JAAS to initialize itself. This flag is enabled by default such that if the **java.security.auth.login.config** system property is not defined then it is set to the location of the **login.config** file on the classpath.

### The _\<jaasCertificateAuthenticationPlugin>_ Element

Provides a JAAS based SSL certificate authentication plugin

#### Properties

Property Name

Type

Description

configuration

_java.lang.String_

Sets the JAAS configuration domain name used

discoverLoginConfig

_boolean_

Enables or disables the auto-discovery of the login.config file for JAAS to initialize itself. This flag is enabled by default such that if the **java.security.auth.login.config** system property is not defined then it is set to the location of the **login.config** file on the classpath.

### The _\<jdbcPersistenceAdapter>_ Element

A {@link PersistenceAdapter} implementation using JDBC for persistence storage. This persistence adapter will correctly remember prepared XA transactions, but it will not keep track of local transaction commits so that operations performed against the Message store are done as a single uow.

#### Properties

Property Name

Type

Description

adapter

_org.apache.activemq.store.jdbc.JDBCAdapter_

brokerName

_java.lang.String_

brokerService

_org.apache.activemq.broker.BrokerService_

cleanupPeriod

_int_

Sets the number of milliseconds until the database is attempted to be cleaned up for durable topics

createTablesOnStartup

_boolean_

Sets whether or not tables are created on startup

dataDirectory

_java.lang.String_

dataDirectoryFile

_java.io.File_

dataSource

_javax.sql.DataSource_

databaseLocker

_org.apache.activemq.store.jdbc.DatabaseLocker_

Sets the database locker strategy to use to lock the database on startup

directory

_java.io.File_

ds

_javax.sql.DataSource_

scheduledThreadPoolExecutor

_java.util.concurrent.ScheduledThreadPoolExecutor_

statements

_org.apache.activemq.store.jdbc.Statements_

usageManager

_org.apache.activemq.usage.SystemUsage_

useDatabaseLock

_boolean_

Sets whether or not an exclusive database lock should be used to enable JDBC Master/Slave. Enabled by default.

useExternalMessageReferences

_boolean_

wireFormat

_org.apache.activemq.wireformat.WireFormat_

### The _\<jmsQueueConnector>_ Element

A Bridge to other JMS Queue providers

#### Properties

Property Name

Type

Description

brokerService

_org.apache.activemq.broker.BrokerService_

One way to configure the local connection - this is called by The BrokerService when the Connector is embedded

inboundMessageConvertor

_org.apache.activemq.network.jms.JmsMesageConvertor_

inboundQueueBridges

(_org.apache.activemq.network.jms.InboundQueueBridge_)*

jndiLocalTemplate

_org.springframework.jndi.JndiTemplate_

jndiOutboundTemplate

_org.springframework.jndi.JndiTemplate_

localConnectionFactoryName

_java.lang.String_

localPassword

_java.lang.String_

localQueueConnection

_javax.jms.QueueConnection_

localQueueConnectionFactory

_javax.jms.QueueConnectionFactory_

localUsername

_java.lang.String_

name

_java.lang.String_

outboundMessageConvertor

_org.apache.activemq.network.jms.JmsMesageConvertor_

outboundPassword

_java.lang.String_

outboundQueueBridges

(_org.apache.activemq.network.jms.OutboundQueueBridge_)*

outboundQueueConnection

_javax.jms.QueueConnection_

outboundQueueConnectionFactory

_javax.jms.QueueConnectionFactory_

outboundQueueConnectionFactoryName

_java.lang.String_

outboundUsername

_java.lang.String_

replyToDestinationCacheSize

_int_

### The _\<jmsTopicConnector>_ Element

A Bridge to other JMS Topic providers

#### Properties

Property Name

Type

Description

brokerService

_org.apache.activemq.broker.BrokerService_

One way to configure the local connection - this is called by The BrokerService when the Connector is embedded

inboundMessageConvertor

_org.apache.activemq.network.jms.JmsMesageConvertor_

inboundTopicBridges

(_org.apache.activemq.network.jms.InboundTopicBridge_)*

jndiLocalTemplate

_org.springframework.jndi.JndiTemplate_

jndiOutboundTemplate

_org.springframework.jndi.JndiTemplate_

localConnectionFactoryName

_java.lang.String_

localPassword

_java.lang.String_

localTopicConnection

_javax.jms.TopicConnection_

localTopicConnectionFactory

_javax.jms.TopicConnectionFactory_

localUsername

_java.lang.String_

name

_java.lang.String_

outboundMessageConvertor

_org.apache.activemq.network.jms.JmsMesageConvertor_

outboundPassword

_java.lang.String_

outboundTopicBridges

(_org.apache.activemq.network.jms.OutboundTopicBridge_)*

outboundTopicConnection

_javax.jms.TopicConnection_

outboundTopicConnectionFactory

_javax.jms.TopicConnectionFactory_

outboundTopicConnectionFactoryName

_java.lang.String_

outboundUsername

_java.lang.String_

replyToDestinationCacheSize

_int_

### The _\<journalPersistenceAdapter>_ Element

An implementation of {@link PersistenceAdapter} designed for use with a {@link Journal} and then check pointing asynchronously on a timeout with some other long term persistent storage.

#### Properties

Property Name

Type

Description

brokerName

_java.lang.String_

directory

_java.io.File_

journal

_org.apache.activeio.journal.Journal_

longTermPersistence

_org.apache.activemq.store.PersistenceAdapter_

maxCheckpointMessageAddSize

_int_

maxCheckpointWorkers

_int_

taskRunnerFactory

_org.apache.activemq.thread.TaskRunnerFactory_

usageManager

_org.apache.activemq.usage.SystemUsage_

useExternalMessageReferences

_boolean_

### The _\<journaledJDBC>_ Element

Creates a default persistence model using the Journal and JDBC

#### Properties

Property Name

Type

Description

adapter

_org.apache.activemq.store.jdbc.JDBCAdapter_

createTablesOnStartup

_boolean_

Sets whether or not tables are created on startup

dataDirectory

_java.lang.String_

dataDirectoryFile

_java.io.File_

dataSource

_javax.sql.DataSource_

jdbcAdapter

_org.apache.activemq.store.jdbc.JDBCPersistenceAdapter_

journal

_org.apache.activeio.journal.Journal_

journalArchiveDirectory

_java.io.File_

journalLogFileSize

_int_

Sets the size of the journal log files

journalLogFiles

_int_

Sets the number of journal log files to use

journalThreadPriority

_int_

Sets the thread priority of the journal thread

statements

_org.apache.activemq.store.jdbc.Statements_

taskRunnerFactory

_org.apache.activemq.thread.TaskRunnerFactory_

useDatabaseLock

_boolean_

Sets whether or not an exclusive database lock should be used to enable JDBC Master/Slave. Enabled by default.

useJournal

_boolean_

Enables or disables the use of the journal. The default is to use the journal

useQuickJournal

_boolean_

Enables or disables the use of quick journal, which keeps messages in the journal and just stores a reference to the messages in JDBC. Defaults to false so that messages actually reside long term in the JDBC database.

### The _\<kahaPersistenceAdapter>_ Element

#### Properties

Property Name

Type

Description

brokerName

_java.lang.String_

directory

_java.io.File_

maxDataFileLength

_long_

persistentIndex

_boolean_

size

_java.util.concurrent.atomic.AtomicLong_

usageManager

_org.apache.activemq.usage.SystemUsage_

### The _\<lDAPAuthorizationMap>_ Element

An {@link AuthorizationMap} which uses LDAP

#### Properties

Property Name

Type

Description

adminAttribute

_java.lang.String_

adminBase

_java.lang.String_

authentication

_java.lang.String_

connectionPassword

_java.lang.String_

connectionProtocol

_java.lang.String_

connectionURL

_java.lang.String_

connectionUsername

_java.lang.String_

context

_javax.naming.directory.DirContext_

initialContextFactory

_java.lang.String_

options

_java.util.Map_

queueSearchMatchingFormat

_java.text.MessageFormat_

queueSearchSubtreeBool

_boolean_

readAttribute

_java.lang.String_

readBase

_java.lang.String_

topicSearchMatchingFormat

_java.text.MessageFormat_

topicSearchSubtreeBool

_boolean_

writeAttribute

_java.lang.String_

writeBase

_java.lang.String_

### The _\<lastImageSubscriptionRecoveryPolicy>_ Element

This implementation of {@link SubscriptionRecoveryPolicy} will only keep the last message.

### The _\<ldapNetworkConnector>_ Element

class to create dynamic network connectors listed in an directory server using the LDAP v3 protocol as defined in RFC 2251, the entries listed in the directory server must implement the ipHost and ipService objectClasses as defined in RFC 2307.

#### Properties

Property Name

Type

Description

base

_java.lang.String_

sets the base LDAP dn used for lookup operations

bridgeTempDestinations

_boolean_

brokerName

_java.lang.String_

brokerService

_org.apache.activemq.broker.BrokerService_

conduitSubscriptions

_boolean_

connectionFilter

_org.apache.activemq.network.ConnectionFilter_

decreaseNetworkConsumerPriority

_boolean_

destinationFilter

_java.lang.String_

dispatchAsync

_boolean_

duplex

_boolean_

durableDestinations

(_java.lang.Object_)*

dynamicOnly

_boolean_

dynamicallyIncludedDestinations

(_java.lang.Object_)*

excludedDestinations

(_java.lang.Object_)*

localUri

_java.net.URI_

name

_java.lang.String_

networkTTL

_int_

objectName

_javax.management.ObjectName_

password

_java.lang.String_

sets the LDAP password for access credentials

prefetchSize

_int_

searchFilter

_java.lang.String_

sets the LDAP search filter as defined in RFC 2254

searchScope

_java.lang.String_

sets the LDAP search scope

staticallyIncludedDestinations

(_java.lang.Object_)*

uri

_java.net.URI_

sets the LDAP server URI

user

_java.lang.String_

sets the LDAP user for access credentials

userName

_java.lang.String_

### The _\<loggingBrokerPlugin>_ Element

A simple Broker interceptor which allows you to enable/disable logging.

#### Properties

Property Name

Type

Description

ackLog

_org.apache.commons.logging.Log_

adminConnectionContext

_org.apache.activemq.broker.ConnectionContext_

log

_org.apache.commons.logging.Log_

next

_org.apache.activemq.broker.Broker_

sendLog

_org.apache.commons.logging.Log_

### The _\<managementContext>_ Element

A Flow provides different dispatch policies within the NMR

#### Properties

Property Name

Type

Description

MBeanServer

_javax.management.MBeanServer_

Get the MBeanServer

connectorPath

_java.lang.String_

connectorPort

_int_

createConnector

_boolean_

createMBeanServer

_boolean_

findTigerMbeanServer

_boolean_

Enables/disables the searching for the Java 5 platform MBeanServer

jmxDomainName

_java.lang.String_

rmiServerPort

_int_

server

_javax.management.MBeanServer_

useMBeanServer

_boolean_

### The _\<masterConnector>_ Element

Connects a Slave Broker to a Master when using [Master Slave](masterslave) for High Availability of messages.

#### Properties

Property Name

Type

Description

brokerService

_org.apache.activemq.broker.BrokerService_

localURI

_java.net.URI_

password

_java.lang.String_

remoteURI

_java.net.URI_

remoteUri

_java.lang.String_

userName

_java.lang.String_

### The _\<memoryPersistenceAdapter>_ Element

#### Properties

Property Name

Type

Description

brokerName

_java.lang.String_

directory

_java.io.File_

usageManager

_org.apache.activemq.usage.SystemUsage_

useExternalMessageReferences

_boolean_

### The _\<memoryUsage>_ Element

Used to keep track of how much of something is being used so that a productive working set usage can be controlled. Main use case is manage memory usage.

#### Properties

Property Name

Type

Description

limit

_long_

Sets the memory limit in bytes. Setting the limit in bytes will set the usagePortion to 0 since the UsageManager is not going to be portion based off the parent. When set using XBean, you can use values such as: "20 mb", "1024 kb", or "1 gb"

limiter

_org.apache.activemq.usage.UsageCapacity_

name

_java.lang.String_

parent

_T_

percentUsage

_int_

percentUsageMinDelta

_int_

Sets the minimum number of percentage points the usage has to change before a UsageListener event is fired by the manager.

pollingTime

_int_

portion

_float_

usage

_long_

usagePortion

_float_

### The _\<messageGroupHashBucketFactory>_ Element

A factory to create instances of {@link SimpleMessageGroupMap} when implementing the [Message Groups](message-groups) functionality.

#### Properties

Property Name

Type

Description

bucketCount

_int_

Sets the number of hash buckets to use for the message group functionality. This is only applicable to using message groups to parallelize processing of a queue while preserving order across an individual JMSXGroupID header value. This value sets the number of hash buckets that will be used (i.e. the maximum possible concurrency).

### The _\<mirroredQueue>_ Element

Creates [Mirrored Queue](mirrored-queues) using a prefix and postfix to define the topic name on which to mirror the queue to.

#### Properties

Property Name

Type

Description

brokerService

_org.apache.activemq.broker.BrokerService_

copyMessage

_boolean_

Sets whether a copy of the message will be sent to each destination. Defaults to true so that the forward destination is set as the destination of the message

postfix

_java.lang.String_

Sets any postix used to identify the queue consumers

prefix

_java.lang.String_

Sets the prefix wildcard used to identify the queue consumers for a given topic

### The _\<multicastNetworkConnector>_ Element

A network connector which uses some kind of multicast-like transport that communicates with potentially many remote brokers over a single logical {@link Transport} instance such as when using multicast. This implementation does not depend on multicast at all; any other group based transport could be used.

#### Properties

Property Name

Type

Description

bridge

_org.apache.activemq.network.DemandForwardingBridgeSupport_

bridgeTempDestinations

_boolean_

brokerName

_java.lang.String_

brokerService

_org.apache.activemq.broker.BrokerService_

conduitSubscriptions

_boolean_

connectionFilter

_org.apache.activemq.network.ConnectionFilter_

decreaseNetworkConsumerPriority

_boolean_

destinationFilter

_java.lang.String_

dispatchAsync

_boolean_

duplex

_boolean_

durableDestinations

(_java.lang.Object_)*

dynamicOnly

_boolean_

dynamicallyIncludedDestinations

(_java.lang.Object_)*

excludedDestinations

(_java.lang.Object_)*

localTransport

_org.apache.activemq.transport.Transport_

localUri

_java.net.URI_

name

_java.lang.String_

networkTTL

_int_

objectName

_javax.management.ObjectName_

password

_java.lang.String_

prefetchSize

_int_

remoteTransport

_org.apache.activemq.transport.Transport_

Sets the remote transport implementation

remoteURI

_java.net.URI_

Sets the remote transport URI to some group transport like `multicast://address:port`

staticallyIncludedDestinations

(_java.lang.Object_)*

userName

_java.lang.String_

### The _\<multicastTraceBrokerPlugin>_ Element

A Broker interceptor which allows you to trace all operations to a Multicast socket.

#### Properties

Property Name

Type

Description

address

_java.net.SocketAddress_

adminConnectionContext

_org.apache.activemq.broker.ConnectionContext_

broadcast

_boolean_

destination

_java.net.URI_

maxTraceDatagramSize

_int_

next

_org.apache.activemq.broker.Broker_

timeToLive

_int_

wireFormat

_org.apache.activemq.wireformat.WireFormat_

wireFormatFactory

_org.apache.activemq.wireformat.WireFormatFactory_

### The _\<networkConnector>_ Element

A network connector which uses a discovery agent to detect the remote brokers available and setup a connection to each available remote broker

#### Properties

Property Name

Type

Description

bridgeTempDestinations

_boolean_

brokerName

_java.lang.String_

brokerService

_org.apache.activemq.broker.BrokerService_

conduitSubscriptions

_boolean_

connectionFilter

_org.apache.activemq.network.ConnectionFilter_

decreaseNetworkConsumerPriority

_boolean_

destinationFilter

_java.lang.String_

discoveryAgent

_org.apache.activemq.transport.discovery.DiscoveryAgent_

discoveryURI

_java.net.URI_

dispatchAsync

_boolean_

duplex

_boolean_

durableDestinations

(_java.lang.Object_)*

dynamicOnly

_boolean_

dynamicallyIncludedDestinations

(_java.lang.Object_)*

excludedDestinations

(_java.lang.Object_)*

localUri

_java.net.URI_

name

_java.lang.String_

networkTTL

_int_

objectName

_javax.management.ObjectName_

password

_java.lang.String_

prefetchSize

_int_

staticallyIncludedDestinations

(_java.lang.Object_)*

uri

_java.net.URI_

userName

_java.lang.String_

### The _\<noSubscriptionRecoveryPolicy>_ Element

This SubscriptionRecoveryPolicy disable recovery of messages.

### The _\<oldestMessageEvictionStrategy>_ Element

An eviction strategy which evicts the oldest message first (which is the default).

#### Properties

Property Name

Type

Description

evictExpiredMessagesHighWatermark

_int_

Sets the high water mark on which we will eagerly evict expired messages from RAM

### The _\<oldestMessageWithLowestPriorityEvictionStrategy>_ Element

An eviction strategy which evicts the oldest message with the lowest priority first.

#### Properties

Property Name

Type

Description

evictExpiredMessagesHighWatermark

_int_

Sets the high water mark on which we will eagerly evict expired messages from RAM

### The _\<oracleJDBCAdapter>_ Element

Implements all the default JDBC operations that are used by the JDBCPersistenceAdapter.

Subclassing is encouraged to override the default implementation of methods to account for differences in JDBC Driver implementations.

The JDBCAdapter inserts and extracts BLOB data using the getBytes()/setBytes() operations.

The databases/JDBC drivers that use this adapter are:

#### Properties

Property Name

Type

Description

batchStatments

_boolean_

statements

_org.apache.activemq.store.jdbc.Statements_

useExternalMessageReferences

_boolean_

### The _\<outboundQueueBridge>_ Element

Create an Outbound Queue Bridge

#### Properties

Property Name

Type

Description

consumer

_javax.jms.MessageConsumer_

consumerConnection

_javax.jms.QueueConnection_

consumerQueue

_javax.jms.Queue_

doHandleReplyTo

_boolean_

jmsConnector

_org.apache.activemq.network.jms.JmsConnector_

jmsMessageConvertor

_org.apache.activemq.network.jms.JmsMesageConvertor_

localQueueName

_java.lang.String_

maximumRetries

_int_

Sets the maximum number of retries if a send fails before closing the bridge

outboundQueueName

_java.lang.String_

producerConnection

_javax.jms.QueueConnection_

producerQueue

_javax.jms.Queue_

selector

_java.lang.String_

### The _\<outboundTopicBridge>_ Element

Create an Outbound Topic Bridge

#### Properties

Property Name

Type

Description

consumer

_javax.jms.MessageConsumer_

consumerConnection

_javax.jms.TopicConnection_

consumerName

_java.lang.String_

consumerTopic

_javax.jms.Topic_

doHandleReplyTo

_boolean_

jmsConnector

_org.apache.activemq.network.jms.JmsConnector_

jmsMessageConvertor

_org.apache.activemq.network.jms.JmsMesageConvertor_

localTopicName

_java.lang.String_

maximumRetries

_int_

Sets the maximum number of retries if a send fails before closing the bridge

outboundTopicName

_java.lang.String_

producerConnection

_javax.jms.TopicConnection_

producerTopic

_javax.jms.Topic_

selector

_java.lang.String_

### The _\<policyEntry>_ Element

Represents an entry in a {@link PolicyMap} for assigning policies to a specific destination or a hierarchical wildcard area of destinations.

#### Properties

Property Name

Type

Description

deadLetterStrategy

_org.apache.activemq.broker.region.policy.DeadLetterStrategy_

Sets the policy used to determine which dead letter queue destination should be used

destination

_org.apache.activemq.command.ActiveMQDestination_

dispatchPolicy

_org.apache.activemq.broker.region.policy.DispatchPolicy_

enableAudit

_boolean_

maxAuditDepth

_int_

maxProducersToAudit

_int_

maxQueueAuditDepth

_int_

memoryLimit

_long_

messageEvictionStrategy

_org.apache.activemq.broker.region.policy.MessageEvictionStrategy_

Sets the eviction strategy used to decide which message to evict when the slow consumer needs to discard messages

messageGroupMapFactory

_org.apache.activemq.broker.region.group.MessageGroupMapFactory_

Sets the factory used to create new instances of {MessageGroupMap} used to implement the [Message Groups](message-groups) functionality.

optimizedDispatch

_boolean_

pendingDurableSubscriberPolicy

_org.apache.activemq.broker.region.policy.PendingDurableSubscriberMessageStoragePolicy_

pendingMessageLimitStrategy

_org.apache.activemq.broker.region.policy.PendingMessageLimitStrategy_

Sets the strategy to calculate the maximum number of messages that are allowed to be pending on consumers (in addition to their prefetch sizes). Once the limit is reached, non-durable topics can then start discarding old messages. This allows us to keep dispatching messages to slow consumers while not blocking fast consumers and discarding the messages oldest first.

pendingQueuePolicy

_org.apache.activemq.broker.region.policy.PendingQueueMessageStoragePolicy_

pendingSubscriberPolicy

_org.apache.activemq.broker.region.policy.PendingSubscriberMessageStoragePolicy_

producerFlowControl

_boolean_

queue

_java.lang.String_

A helper method to set the destination from a configuration file

sendAdvisoryIfNoConsumers

_boolean_

Sends an advisory message if a non-persistent message is sent and there are no active consumers

subscriptionRecoveryPolicy

_org.apache.activemq.broker.region.policy.SubscriptionRecoveryPolicy_

topic

_java.lang.String_

A helper method to set the destination from a configuration file

### The _\<policyMap>_ Element

Represents a destination based configuration of policies so that individual destinations or wildcard hierarchies of destinations can be configured using different policies.

#### Properties

Property Name

Type

Description

defaultEntry

_org.apache.activemq.broker.region.policy.PolicyEntry_

entries

(_java.lang.Object_)*

A helper method to allow the destination map to be populated from a dependency injection framework such as Spring

policyEntries

(_java.lang.Object_)*

Sets the individual entries on the policy map

### The _\<prefetchPolicy>_ Element

Defines the prefetch message policies for different types of consumers

#### Properties

Property Name

Type

Description

all

_int_

durableTopicPrefetch

_int_

inputStreamPrefetch

_int_

maximumPendingMessageLimit

_int_

Sets how many messages a broker will keep around, above the prefetch limit, for non-durable topics before starting to discard older messages.

optimizeDurableTopicPrefetch

_int_

queueBrowserPrefetch

_int_

queuePrefetch

_int_

topicPrefetch

_int_

### The _\<prefetchRatePendingMessageLimitStrategy>_ Element

This PendingMessageLimitStrategy sets the maximum pending message limit value to be a multiplier of the prefetch limit of the subscription.

#### Properties

Property Name

Type

Description

multiplier

_double_

Sets the multiplier of the prefetch size which will be used to define the maximum number of pending messages for non-durable topics before messages are discarded.

### The _\<proxyConnector>_ Element

#### Properties

Property Name

Type

Description

bind

_java.net.URI_

localUri

_java.net.URI_

name

_java.lang.String_

remote

_java.net.URI_

server

_org.apache.activemq.transport.TransportServer_

### The _\<queryBasedSubscriptionRecoveryPolicy>_ Element

This implementation of {@link SubscriptionRecoveryPolicy} will perform a user specific query mechanism to load any messages they may have missed.

#### Properties

Property Name

Type

Description

query

_org.apache.activemq.broker.region.policy.MessageQuery_

Sets the query strategy to load initial messages

### The _\<queue>_ Element

An ActiveMQ Queue

#### Properties

Property Name

Type

Description

compositeDestinations

(_org.apache.activemq.command.ActiveMQDestination_)*

name

_java.lang.String_

physicalName

_java.lang.String_

properties

_java.util.Properties_

Get the properties from this instance for storing in JNDI

### The _\<redeliveryPolicy>_ Element

Configuration options used to control how messages are re-delivered when they are rolled back.

#### Properties

Property Name

Type

Description

backOffMultiplier

_short_

collisionAvoidancePercent

_short_

initialRedeliveryDelay

_long_

maximumRedeliveries

_int_

useCollisionAvoidance

_boolean_

useExponentialBackOff

_boolean_

### The _\<roundRobinDispatchPolicy>_ Element

Simple dispatch policy that sends a message to every subscription that matches the message.

### The _\<sharedDeadLetterStrategy>_ Element

A default implementation of {@link DeadLetterStrategy} which uses a constant destination.

#### Properties

Property Name

Type

Description

deadLetterQueue

_org.apache.activemq.command.ActiveMQDestination_

processExpired

_boolean_

processNonPersistent

_boolean_

### The _\<simpleAuthenticationPlugin>_ Element

Provides a simple authentication plugin

#### Properties

Property Name

Type

Description

userGroups

_java.util.Map_

Sets the groups a user is in. The key is the user name and the value is a Set of groups

userPasswords

_java.util.Map_

Sets the map indexed by user name with the value the password

users

(_java.lang.Object_)*

Sets individual users for authentication

### The _\<simpleAuthorizationMap>_ Element

An AuthorizationMap which is configured with individual DestinationMaps for each operation.

#### Properties

Property Name

Type

Description

adminACLs

_org.apache.activemq.filter.DestinationMap_

readACLs

_org.apache.activemq.filter.DestinationMap_

tempDestinationAuthorizationEntry

_org.apache.activemq.security.TempDestinationAuthorizationEntry_

writeACLs

_org.apache.activemq.filter.DestinationMap_

### The _\<simpleDispatchPolicy>_ Element

Simple dispatch policy that sends a message to every subscription that matches the message.

### The _\<simpleJmsMessageConvertor>_ Element

Converts Message from one JMS to another

#### Properties

Property Name

Type

Description

connection

_javax.jms.Connection_

### The _\<simpleMessageGroupMapFactory>_ Element

A factory to create instances of {@link SimpleMessageGroupMap} when implementing the [Message Groups](message-groups) functionality.

### The _\<statements>_ Element

#### Properties

Property Name

Type

Description

addMessageStatement

_java.lang.String_

binaryDataType

_java.lang.String_

containerNameDataType

_java.lang.String_

createDurableSubStatement

_java.lang.String_

createSchemaStatements

(_java.lang.String_)*

deleteOldMessagesStatement

_java.lang.String_

deleteSubscriptionStatement

_java.lang.String_

destinationMessageCountStatement

_java.lang.String_

dropSchemaStatements

(_java.lang.String_)*

durableSubAcksTableName

_java.lang.String_

durableSubscriberMessageCountStatement

_java.lang.String_

findAllDestinationsStatement

_java.lang.String_

findAllDurableSubMessagesStatement

_java.lang.String_

findAllDurableSubsStatement

_java.lang.String_

findAllMessagesStatement

_java.lang.String_

findDurableSubMessagesStatement

_java.lang.String_

findDurableSubStatement

_java.lang.String_

findLastSequenceIdInAcksStatement

_java.lang.String_

findLastSequenceIdInMsgsStatement

_java.lang.String_

findMessageSequenceIdStatement

_java.lang.String_

findMessageStatement

_java.lang.String_

findNextMessagesStatement

_java.lang.String_

lastAckedDurableSubscriberMessageStatement

_java.lang.String_

lockCreateStatement

_java.lang.String_

lockTableName

_java.lang.String_

lockUpdateStatement

_java.lang.String_

longDataType

_java.lang.String_

messageTableName

_java.lang.String_

msgIdDataType

_java.lang.String_

nextDurableSubscriberMessageStatement

_java.lang.String_

removeAllMessagesStatement

_java.lang.String_

removeAllSubscriptionsStatement

_java.lang.String_

removeMessageStatment

_java.lang.String_

sequenceDataType

_java.lang.String_

stringIdDataType

_java.lang.String_

tablePrefix

_java.lang.String_

updateLastAckOfDurableSubStatement

_java.lang.String_

updateMessageStatement

_java.lang.String_

useExternalMessageReferences

_boolean_

useLockCreateWhereClause

_boolean_

### The _\<storeCursor>_ Element

Pending messages

### The _\<storeDurableSubscriberCursor>_ Element

Pending messages for a durable

### The _\<storeUsage>_ Element

Used to keep track of how much of something is being used so that a productive working set usage can be controlled. Main use case is manage memory usage.

#### Properties

Property Name

Type

Description

limit

_long_

Sets the memory limit in bytes. Setting the limit in bytes will set the usagePortion to 0 since the UsageManager is not going to be portion based off the parent. When set using XBean, you can use values such as: "20 mb", "1024 kb", or "1 gb"

limiter

_org.apache.activemq.usage.UsageCapacity_

name

_java.lang.String_

parent

_T_

percentUsage

_int_

percentUsageMinDelta

_int_

Sets the minimum number of percentage points the usage has to change before a UsageListener event is fired by the manager.

pollingTime

_int_

store

_org.apache.activemq.store.PersistenceAdapter_

usagePortion

_float_

### The _\<streamJDBCAdapter>_ Element

This JDBCAdapter inserts and extracts BLOB data using the setBinaryStream()/getBinaryStream() operations. The databases/JDBC drivers that use this adapter are:

*   Axion

#### Properties

Property Name

Type

Description

batchStatments

_boolean_

statements

_org.apache.activemq.store.jdbc.Statements_

useExternalMessageReferences

_boolean_

### The _\<strictOrderDispatchPolicy>_ Element

Dispatch policy that causes every subscription to see messages in the same order.

### The _\<systemUsage>_ Element

Holder for Usage instances for memory, store and temp files Main use case is manage memory usage.

#### Properties

Property Name

Type

Description

adapter

_org.apache.activemq.store.PersistenceAdapter_

memoryUsage

_org.apache.activemq.usage.MemoryUsage_

name

_java.lang.String_

parent

_org.apache.activemq.usage.SystemUsage_

sendFailIfNoSpace

_boolean_

Sets whether or not a send() should fail if there is no space free. The default value is false which means to block the send() method until space becomes available

sendFailIfNoSpaceExplicitySet

_boolean_

storeUsage

_org.apache.activemq.usage.StoreUsage_

tempStore

_org.apache.activemq.kaha.Store_

tempUsage

_org.apache.activemq.usage.TempUsage_

### The _\<tempDestinationAuthorizationEntry>_ Element

Represents an entry in a {@link DefaultAuthorizationMap} for assigning different operations (read, write, admin) of user roles to a temporary destination

#### Properties

Property Name

Type

Description

admin

_java.lang.String_

adminACLs

(_java.lang.Object_)*

destination

_org.apache.activemq.command.ActiveMQDestination_

groupClass

_java.lang.String_

queue

_java.lang.String_

A helper method to set the destination from a configuration file

read

_java.lang.String_

readACLs

(_java.lang.Object_)*

topic

_java.lang.String_

A helper method to set the destination from a configuration file

write

_java.lang.String_

writeACLs

(_java.lang.Object_)*

### The _\<tempUsage>_ Element

Used to keep track of how much of something is being used so that a productive working set usage can be controlled. Main use case is manage memory usage.

#### Properties

Property Name

Type

Description

limit

_long_

Sets the memory limit in bytes. Setting the limit in bytes will set the usagePortion to 0 since the UsageManager is not going to be portion based off the parent. When set using XBean, you can use values such as: "20 mb", "1024 kb", or "1 gb"

limiter

_org.apache.activemq.usage.UsageCapacity_

name

_java.lang.String_

parent

_T_

percentUsage

_int_

percentUsageMinDelta

_int_

Sets the minimum number of percentage points the usage has to change before a UsageListener event is fired by the manager.

pollingTime

_int_

store

_org.apache.activemq.kaha.Store_

usagePortion

_float_

### The _\<timeStampingBrokerPlugin>_ Element

A Broker interceptor which updates a JMS Client's timestamp on the message with a broker timestamp. Useful when the clocks on client machines are known to not be correct and you can only trust the time set on the broker machines. Enabling this plugin will break JMS compliance since the timestamp that the producer sees on the messages after as send() will be different from the timestamp the consumer will observe when he receives the message. This plugin is not enabled in the default ActiveMQ configuration.

#### Properties

Property Name

Type

Description

adminConnectionContext

_org.apache.activemq.broker.ConnectionContext_

next

_org.apache.activemq.broker.Broker_

### The _\<timedSubscriptionRecoveryPolicy>_ Element

This implementation of {@link SubscriptionRecoveryPolicy} will keep a timed buffer of messages around in memory and use that to recover new subscriptions.

#### Properties

Property Name

Type

Description

recoverDuration

_long_

### The _\<topic>_ Element

An ActiveMQ Topic

#### Properties

Property Name

Type

Description

compositeDestinations

(_org.apache.activemq.command.ActiveMQDestination_)*

name

_java.lang.String_

physicalName

_java.lang.String_

properties

_java.util.Properties_

Get the properties from this instance for storing in JNDI

### The _\<transportConnector>_ Element

#### Properties

Property Name

Type

Description

broker

_org.apache.activemq.broker.Broker_

brokerInfo

_org.apache.activemq.command.BrokerInfo_

brokerName

_java.lang.String_

connectUri

_java.net.URI_

disableAsyncDispatch

_boolean_

discoveryAgent

_org.apache.activemq.transport.discovery.DiscoveryAgent_

discoveryUri

_java.net.URI_

enableStatusMonitor

_boolean_

messageAuthorizationPolicy

_org.apache.activemq.security.MessageAuthorizationPolicy_

Sets the policy used to decide if the current connection is authorized to consume a given message

name

_java.lang.String_

server

_org.apache.activemq.transport.TransportServer_

taskRunnerFactory

_org.apache.activemq.thread.TaskRunnerFactory_

uri

_java.net.URI_

Sets the server transport URI to use if there is not a {@link TransportServer} configured via the {@link #setServer(TransportServer)} method. This value is used to lazy create a {@link TransportServer} instance

### The _\<udpTraceBrokerPlugin>_ Element

A Broker interceptor which allows you to trace all operations to a UDP socket.

#### Properties

Property Name

Type

Description

address

_java.net.SocketAddress_

adminConnectionContext

_org.apache.activemq.broker.ConnectionContext_

broadcast

_boolean_

destination

_java.net.URI_

maxTraceDatagramSize

_int_

next

_org.apache.activemq.broker.Broker_

wireFormat

_org.apache.activemq.wireformat.WireFormat_

wireFormatFactory

_org.apache.activemq.wireformat.WireFormatFactory_

### The _\<usageCapacity>_ Element

Identify if a limit has been reached

#### Properties

Property Name

Type

Description

limit

_long_

### The _\<virtualDestinationInterceptor>_ Element

Implements [Virtual Topics](virtual-destinations).

#### Properties

Property Name

Type

Description

virtualDestinations

(_org.apache.activemq.broker.region.virtual.VirtualDestination_)*

### The _\<virtualTopic>_ Element

Creates [Virtual Topics](virtual-destinations) using a prefix and postfix. The virtual destination creates a wildcard that is then used to look up all active queue subscriptions which match.

#### Properties

Property Name

Type

Description

name

_java.lang.String_

postfix

_java.lang.String_

Sets any postix used to identify the queue consumers

prefix

_java.lang.String_

Sets the prefix wildcard used to identify the queue consumers for a given topic

### The _\<vmCursor>_ Element

Pending messages held

### The _\<vmDurableCursor>_ Element

Pending

### The _\<vmQueueCursor>_ Element

Pending messages

### The _\<xaConnectionFactory>_ Element

A [Spring](http://www.springframework.org/) enhanced XA connection factory which will automatically use the Spring bean name as the clientIDPrefix property so that connections created have client IDs related to your Spring.xml file for easier comprehension from [JMX](jmx).

#### Properties

Property Name

Type

Description

alwaysSessionAsync

_boolean_

If this flag is set then a separate thread is not used for dispatching messages for each Session in the Connection. However, a separate thread is always used if there is more than one session, or the session isn't in auto acknowledge or duplicates ok mode

alwaysSyncSend

_boolean_

Set true if always require messages to be sync sent

beanName

_java.lang.String_

blobTransferPolicy

_org.apache.activemq.blob.BlobTransferPolicy_

Sets the policy used to describe how out-of-band BLOBs (Binary Large OBjects) are transferred from producers to brokers to consumers

brokerURL

_java.lang.String_

Sets the [connection URL](configuring-transports) used to connect to the ActiveMQ broker.

clientID

_java.lang.String_

Sets the JMS clientID to use for the created connection. Note that this can only be used by one connection at once so generally its a better idea to set the clientID on a Connection

clientIDPrefix

_java.lang.String_

Sets the prefix used by autogenerated JMS Client ID values which are used if the JMS client does not explicitly specify on.

clientIdGenerator

_org.apache.activemq.util.IdGenerator_

closeTimeout

_int_

Sets the timeout before a close is considered complete. Normally a close() on a connection waits for confirmation from the broker; this allows that operation to timeout to save the client hanging if there is no broker

copyMessageOnSend

_boolean_

Should a JMS message be copied to a new JMS Message object as part of the send() method in JMS. This is enabled by default to be compliant with the JMS specification. You can disable it if you do not mutate JMS messages after they are sent for a performance boost

disableTimeStampsByDefault

_boolean_

Sets whether or not timestamps on messages should be disabled or not. If you disable them it adds a small performance boost.

dispatchAsync

_boolean_

Enables or disables the default setting of whether or not consumers have their messages [dispatched synchronously or asynchronously by the broker](consumer-dispatch-async). For non-durable topics for example we typically dispatch synchronously by default to minimize context switches which boost performance. However sometimes its better to go slower to ensure that a single blocked consumer socket does not block delivery to other consumers.

exclusiveConsumer

_boolean_

Enables or disables whether or not queue consumers should be exclusive or not for example to preserve ordering when not using [Message Groups](message-groups)

nestedMapAndListEnabled

_boolean_

Enables/disables whether or not Message properties and MapMessage entries support [Nested Structures](structured-message-properties-and-mapmessages) of Map and List objects

objectMessageSerializationDefered

_boolean_

When an object is set on an ObjectMessage, the JMS spec requires the object to be serialized by that set method. Enabling this flag causes the object to not get serialized. The object may subsequently get serialized if the message needs to be sent over a socket or stored to disk.

optimizeAcknowledge

_boolean_

optimizedMessageDispatch

_boolean_

If this flag is set then an larger prefetch limit is used - only applicable for durable topic subscribers.

password

_java.lang.String_

Sets the JMS password used for connections created from this factory

prefetchPolicy

_org.apache.activemq.ActiveMQPrefetchPolicy_

Sets the [prefetch policy](what-is-the-prefetch-limit-for) for consumers created by this connection.

producerWindowSize

_int_

properties

_java.util.Properties_

Get the properties from this instance for storing in JNDI

redeliveryPolicy

_org.apache.activemq.RedeliveryPolicy_

Sets the global redelivery policy to be used when a message is delivered but the session is rolled back

statsEnabled

_boolean_

transformer

_org.apache.activemq.MessageTransformer_

Sets the transformer used to transform messages before they are sent on to the JMS bus or when they are received from the bus but before they are delivered to the JMS client

transportListener

_org.apache.activemq.transport.TransportListener_

Allows a listener to be configured on the ConnectionFactory so that when this factory is used with frameworks which don't expose the Connection such as Spring JmsTemplate, you can still register a transport listener.

useAsyncSend

_boolean_

Forces the use of [Async Sends](async-sends) which adds a massive performance boost; but means that the send() method will return immediately whether the message has been sent or not which could lead to message loss.

useBeanNameAsClientIdPrefix

_boolean_

useCompression

_boolean_

Enables the use of compression of the message bodies

useRetroactiveConsumer

_boolean_

Sets whether or not retroactive consumers are enabled. Retroactive consumers allow non-durable topic subscribers to receive old messages that were published before the non-durable subscriber started.

userName

_java.lang.String_

Sets the JMS userName used by connections created by this factory

warnAboutUnstartedConnectionTimeout

_long_

Enables the timeout from a connection creation to when a warning is generated if the connection is not properly started via {@link Connection#start()} and a message is received by a consumer. It is a very common gotcha to forget to [start the connection](i-am-not-receiving-any-messages-what-is-wrong) so this option makes the default case to create a warning if the user forgets. To disable the warning just set the value to < 0 (say -1).

watchTopicAdvisories

_boolean_

### Element Index

_\<amqPersistenceAdapter>_

An implementation of {@link PersistenceAdapter} designed for use with a {@link Journal} and then check pointing asynchronously on a timeout with some other long term persistent storage.

_\<amqPersistenceAdapterFactory>_

An implementation of {@link PersistenceAdapterFactory}

_\<authenticationUser>_

A helper object used to configure simple authentiaction plugin

_\<authorizationEntry>_

Represents an entry in a {@link DefaultAuthorizationMap} for assigning different operations (read, write, admin) of user roles to a specific destination or a hierarchical wildcard area of destinations.

_\<authorizationMap>_

Represents a destination based configuration of policies so that individual destinations or wildcard hierarchies of destinations can be configured using different policies. Each entry in the map represents the authorization ACLs for each operation.

_\<authorizationPlugin>_

An authorization plugin where each operation on a destination is checked against an authorizationMap

_\<axionJDBCAdapter>_

Axion specific Adapter. Axion does not seem to support ALTER statements or sub-selects. This means: - We cannot auto upgrade the schema was we roll out new versions of ActiveMQ - We cannot delete durable sub messages that have be acknowledged by all consumers.

_\<blobJDBCAdapter>_

This JDBCAdapter inserts and extracts BLOB data using the getBlob()/setBlob() operations. This is a little more involved since to insert a blob you have to: 1: insert empty blob. 2: select the blob 3: finally update the blob with data value. The databases/JDBC drivers that use this adapter are:

_\<broker>_

An ActiveMQ Message Broker. It consists of a number of transport connectors, network connectors and a bunch of properties which can be used to configure the broker as its lazily created.

_\<bytesJDBCAdapter>_

This JDBCAdapter inserts and extracts BLOB data using the setBytes()/getBytes() operations. The databases/JDBC drivers that use this adapter are:

_\<commandAgent>_

An agent which listens to commands on a JMS destination

_\<compositeDemandForwardingBridge>_

A demand forwarding bridge which works with multicast style transports where a single Transport could be communicating with multiple remote brokers

_\<compositeQueue>_

Represents a virtual queue which forwards to a number of other destinations.

_\<compositeTopic>_

Represents a virtual topic which forwards to a number of other destinations.

_\<connectionDotFilePlugin>_

A [DOT](http://www.graphviz.org/) file creator plugin which creates a DOT file showing the current connections

_\<connectionFactory>_

A [Spring](http://www.springframework.org/) enhanced connection factory which will automatically use the Spring bean name as the clientIDPrefix property so that connections created have client IDs related to your Spring.xml file for easier comprehension from [JMX](jmx).

_\<constantPendingMessageLimitStrategy>_

This PendingMessageLimitStrategy is configured to a constant value for all subscriptions.

_\<db2JDBCAdapter>_

_\<defaultJDBCAdapter>_

Implements all the default JDBC operations that are used by the JDBCPersistenceAdapter.

sub-classing is encouraged to override the default implementation of methods to account for differences in JDBC Driver implementations.

The JDBCAdapter inserts and extracts BLOB data using the getBytes()/setBytes() operations.

The databases/JDBC drivers that use this adapter are:

_\<defaultUsageCapacity>_

Identify if a limit has been reached

_\<demandForwardingBridge>_

Forwards messages from the local broker to the remote broker based on demand.

_\<destinationDotFilePlugin>_

A [DOT](http://www.graphviz.org/) file creator plugin which creates a DOT file showing the current topic & queue hierarchies.

_\<destinationEntry>_

A default entry in a DestinationMap which holds a single value.

_\<fileCursor>_

Pending messages

_\<fileDurableSubscriberCursor>_

Pending messages for durable subscribers

_\<fileQueueCursor>_

Pending

_\<filteredDestination>_

Represents a destination which is filtered using some predicate such as a selector so that messages are only dispatched to the destination if they match the filter.

_\<fixedCountSubscriptionRecoveryPolicy>_

This implementation of {@link SubscriptionRecoveryPolicy} will keep a fixed count of last messages.

_\<fixedSizedSubscriptionRecoveryPolicy>_

This implementation of {@link SubscriptionRecoveryPolicy} will keep a fixed amount of memory available in RAM for message history which is evicted in time order.

_\<forwardingBridge>_

Forwards all messages from the local broker to the remote broker.

_\<imageBasedJDBCAdaptor>_

Provides JDBCAdapter since that uses IMAGE datatype to hold binary data. The databases/JDBC drivers that use this adapter are:

*   Sybase
*   MS SQL

_\<inboundQueueBridge>_

Create an Inbound Queue Bridge

_\<inboundTopicBridge>_

Create an Inbound Topic Bridge

_\<individualDeadLetterStrategy>_

A {@link DeadLetterStrategy} where each destination has its own individual DLQ using the subject naming hierarchy.

_\<informixJDBCAdapter>_

JDBC Adapter for Informix database. Because Informix database restricts length of composite primary keys, length of _container name_ field and _subscription id_ field must be reduced to 150 characters. Therefore be sure not to use longer names for container name and subscription id than 150 characters.

_\<jaasAuthenticationPlugin>_

Provides a JAAS based authentication plugin

_\<jaasCertificateAuthenticationPlugin>_

Provides a JAAS based SSL certificate authentication plugin

_\<jdbcPersistenceAdapter>_

A {@link PersistenceAdapter} implementation using JDBC for persistence storage. This persistence adapter will correctly remember prepared XA transactions, but it will not keep track of local transaction commits so that operations performed against the Message store are done as a single uow.

_\<jmsQueueConnector>_

A Bridge to other JMS Queue providers

_\<jmsTopicConnector>_

A Bridge to other JMS Topic providers

_\<journalPersistenceAdapter>_

An implementation of {@link PersistenceAdapter} designed for use with a {@link Journal} and then check pointing asynchronously on a timeout with some other long term persistent storage.

_\<journaledJDBC>_

Creates a default persistence model using the Journal and JDBC

_\<kahaPersistenceAdapter>_

_\<lDAPAuthorizationMap>_

An {@link AuthorizationMap} which uses LDAP

_\<lastImageSubscriptionRecoveryPolicy>_

This implementation of {@link SubscriptionRecoveryPolicy} will only keep the last message.

_\<ldapNetworkConnector>_

class to create dynamic network connectors listed in an directory server using the LDAP v3 protocol as defined in RFC 2251, the entries listed in the directory server must implement the ipHost and ipService objectClasses as defined in RFC 2307.

_\<loggingBrokerPlugin>_

A simple Broker interceptor which allows you to enable/disable logging.

_\<managementContext>_

A Flow provides different dispatch policies within the NMR

_\<masterConnector>_

Connects a Slave Broker to a Master when using [Master Slave](masterslave) for High Availability of messages.

_\<memoryPersistenceAdapter>_

_\<memoryUsage>_

Used to keep track of how much of something is being used so that a productive working set usage can be controlled. Main use case is manage memory usage.

_\<messageGroupHashBucketFactory>_

A factory to create instances of {@link SimpleMessageGroupMap} when implementing the [Message Groups](message-groups) functionality.

_\<mirroredQueue>_

Creates [Mirrored Queue](mirrored-queues) using a prefix and postfix to define the topic name on which to mirror the queue to.

_\<multicastNetworkConnector>_

A network connector which uses some kind of multicast-like transport that communicates with potentially many remote brokers over a single logical {@link Transport} instance such as when using multicast. This implementation does not depend on multicast at all; any other group based transport could be used.

_\<multicastTraceBrokerPlugin>_

A Broker interceptor which allows you to trace all operations to a Multicast socket.

_\<networkConnector>_

A network connector which uses a discovery agent to detect the remote brokers available and setup a connection to each available remote broker

_\<noSubscriptionRecoveryPolicy>_

This SubscriptionRecoveryPolicy disable recovery of messages.

_\<oldestMessageEvictionStrategy>_

An eviction strategy which evicts the oldest message first (which is the default).

_\<oldestMessageWithLowestPriorityEvictionStrategy>_

An eviction strategy which evicts the oldest message with the lowest priority first.

_\<oracleJDBCAdapter>_

Implements all the default JDBC operations that are used by the JDBCPersistenceAdapter.

Subclassing is encouraged to override the default implementation of methods to account for differences in JDBC Driver implementations.

The JDBCAdapter inserts and extracts BLOB data using the getBytes()/setBytes() operations.

The databases/JDBC drivers that use this adapter are:

_\<outboundQueueBridge>_

Create an Outbound Queue Bridge

_\<outboundTopicBridge>_

Create an Outbound Topic Bridge

_\<policyEntry>_

Represents an entry in a {@link PolicyMap} for assigning policies to a specific destination or a hierarchical wildcard area of destinations.

_\<policyMap>_

Represents a destination based configuration of policies so that individual destinations or wildcard hierarchies of destinations can be configured using different policies.

_\<prefetchPolicy>_

Defines the prefetch message policies for different types of consumers

_\<prefetchRatePendingMessageLimitStrategy>_

This PendingMessageLimitStrategy sets the maximum pending message limit value to be a multiplier of the prefetch limit of the subscription.

_\<proxyConnector>_

_\<queryBasedSubscriptionRecoveryPolicy>_

This implementation of {@link SubscriptionRecoveryPolicy} will perform a user specific query mechanism to load any messages they may have missed.

_\<queue>_

An ActiveMQ Queue

_\<redeliveryPolicy>_

Configuration options used to control how messages are re-delivered when they are rolled back.

_\<roundRobinDispatchPolicy>_

Simple dispatch policy that sends a message to every subscription that matches the message.

_\<sharedDeadLetterStrategy>_

A default implementation of {@link DeadLetterStrategy} which uses a constant destination.

_\<simpleAuthenticationPlugin>_

Provides a simple authentication plugin

_\<simpleAuthorizationMap>_

An AuthorizationMap which is configured with individual DestinationMaps for each operation.

_\<simpleDispatchPolicy>_

Simple dispatch policy that sends a message to every subscription that matches the message.

_\<simpleJmsMessageConvertor>_

Converts Message from one JMS to another

_\<simpleMessageGroupMapFactory>_

A factory to create instances of {@link SimpleMessageGroupMap} when implementing the [Message Groups](message-groups) functionality.

_\<statements>_

_\<storeCursor>_

Pending messages

_\<storeDurableSubscriberCursor>_

Pending messages for a durable

_\<storeUsage>_

Used to keep track of how much of something is being used so that a productive working set usage can be controlled. Main use case is manage memory usage.

_\<streamJDBCAdapter>_

This JDBCAdapter inserts and extracts BLOB data using the setBinaryStream()/getBinaryStream() operations. The databases/JDBC drivers that use this adapter are:

*   Axion

_\<strictOrderDispatchPolicy>_

Dispatch policy that causes every subscription to see messages in the same order.

_\<systemUsage>_

Holder for Usage instances for memory, store and temp files Main use case is manage memory usage.

_\<tempDestinationAuthorizationEntry>_

Represents an entry in a {@link DefaultAuthorizationMap} for assigning different operations (read, write, admin) of user roles to a temporary destination

_\<tempUsage>_

Used to keep track of how much of something is being used so that a productive working set usage can be controlled. Main use case is manage memory usage.

_\<timeStampingBrokerPlugin>_

A Broker interceptor which updates a JMS Client's timestamp on the message with a broker timestamp. Useful when the clocks on client machines are known to not be correct and you can only trust the time set on the broker machines. Enabling this plugin will break JMS compliance since the timestamp that the producer sees on the messages after as send() will be different from the timestamp the consumer will observe when he receives the message. This plugin is not enabled in the default ActiveMQ configuration.

_\<timedSubscriptionRecoveryPolicy>_

This implementation of {@link SubscriptionRecoveryPolicy} will keep a timed buffer of messages around in memory and use that to recover new subscriptions.

_\<topic>_

An ActiveMQ Topic

_\<transportConnector>_

_\<udpTraceBrokerPlugin>_

A Broker interceptor which allows you to trace all operations to a UDP socket.

_\<usageCapacity>_

Identify if a limit has been reached

_\<virtualDestinationInterceptor>_

Implements [Virtual Topics](virtual-destinations).

_\<virtualTopic>_

Creates [Virtual Topics](virtual-destinations) using a prefix and postfix. The virtual destination creates a wildcard that is then used to look up all active queue subscriptions which match.

_\<vmCursor>_

Pending messages held

_\<vmDurableCursor>_

Pending

_\<vmQueueCursor>_

Pending messages

_\<xaConnectionFactory>_

A [Spring](http://www.springframework.org/) enhanced XA connection factory which will automatically use the Spring bean name as the clientIDPrefix property so that connections created have client IDs related to your Spring.xml file for easier comprehension from [JMX](jmx).

