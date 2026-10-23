---
title: "ActiveMQ Performance Module Users Manual"
slug: /activemq-classic-performance-module-users-manual
---


Running Maven 2 Performance Test
--------------------------------

This [Maven 2](http://maven.apache.org) plugin allows you to run performance tests easily using the Maven command line or run tests automatically in [Continuum](http://maven.apache.org/continuum/)

### Prerequisites

Starting with ActiveMQ 5.5 and above the plugin can be obtained from maven or if you download the src from GIT you can build it yourself. Start with a complete build of Apache ActiveMQ and then do
```
cd activemq-tooling
mvn clean install
```
To run the following Maven goals make sure you are inside a Maven2 project directory where its POM is enabled with the Maven2 plugin.

### Getting started

To get started, switch to the [activemq-perftest directory from the sandbox repo,](http://svn.apache.org/repos/asf/activemq/sandbox/activemq-perftest/) or to a directory containing your own Maven POM with the following plugin defined:
```
<plugin>
  <groupId>org.apache.activemq.tooling</groupId>
  <artifactId>activemq-perf-maven-plugin</artifactId>
  <version>${activemq-version}</version>
</plugin>
```
In ActiveMQ versions prior to 5.8, the artifactId was `maven-activemq-perf-plugin`.

Type the following commands into separate shells (the broker line here is optional if you have a preconfigured one running):
```
mvn activemq-perf:broker -Durl=broker:tcp://localhost:61616
mvn activemq-perf:consumer
mvn activemq-perf:producer
```
If you run the broker on a different machine then change the producer and consumer commands to
```
mvn activemq-perf:consumer -Dfactory.brokerURL=tcp://host:port
mvn activemq-perf:producer -Dfactory.brokerURL=tcp://host:port
```
where you switch host:port with the actual values.

When you run the producer & consumer, it generates an XML performance report in the current directory, so that you can graph and chart the results etc. See the [sample report](sample-report)

### Maven goals

Goal|Description
---|---
`activemq-perf:broker`|Starts broker using the activemq configuration file located in "src\\main\\resources\\broker-conf" where the default config is activemq.xml.  Parameters:  `-DconfigType` - specifies the type of configuration to use. Its value must be one of the filename in the "..\\broker-config" directory (e.g. -DconfigType=kaha).  `-DconfigFile` - path to config file other than those in "src\\..\\broker-config".e.g -DconfigFile=c:\\dir\\activemq.xml)
`activemq-perf:consumer`|Starts the consumer's performance testing. The generated report file can be found in the directory specified in the parameter "sysTest.reportDir".
`activemq-perf:producer`|Starts the producer's performance testing. The generated report file can be found in the directory specified in the parameter "sysTest.reportDir".

> **Note**:The parameters for both consumer and producers are described in the next section. ([sample report](sample-report))

### Configuration for running a system of JMS Clients (Producer/Consumer)

Configuration Key|Default Value|Description
---|---|---
`sysTest.propsConfigFile`|null|Use properties specified externally in a key=value properties file
`sysTest.clientPrefix`|JmsConsumer or JmsProducer|Specifies the client name prefix that will be use for each system. By default it will use JmsConsumer for consumer systems, and JmsProducer for producer systems.
`sysTest.numClients`|1|Specifies the number of JMS Clients to start.
`sysTest.totalDests`|1|Specifies the total number of destinations to use for the whole system test.
`sysTest.destDistro`|all|Specifies how to distribute the destinations to the clients. Available values are (Invalid value will result to using the default value 'all'): 'all' - All clients will send/receive to all destinations. i.e. if there are 2 producers and 5 destinations, each producer will send a message to each individual destination. 'equal' - Clients will equally divide the destinations among themselves. i.e. if there are 2 producers and 5 destinations, each producer will send messages to 2 destinations. The fifth destination will not be used. 'divide' - Clients will divide the destinations among themselves regardless of equality. i.e. if there are 2 producers and 5 destinations, producer 1 will send to 3 destinations, while producer 2 will send to 2 destinations.
`sysTest.reportDir`|./|The directory where the sampler report will be saved.
`sysTest.reportName`|null|The default filename the sampler report will be saved us. There is usually no need to set this setting as the default value will create either JmsProducer_numClients#_numDests#_<Dest Distro>.xml and JmsConsumer\_numClients#\_numDests#_<Dest Distro>.xml, where # represents the number of clients and destinations set for that system, and <Dest Distro> specifies the destination distribution for the system.
`sysTest.reportType`|xml|Specifies the manner of generating the performance report. Available values are: 'xml' - Write the performance report into an xml file. Specified by reportDirectory and reportName. A verbose summary will also be displayed at the end of the test. 'verbose' - All information will be written to the standard output, which includes settings, sampler data, and performance summary.
`sysTest.samplers`|tp,cpu|Specifies the samplers that will be started. To use enable more than one sampler, separate the values by commas without spaces. Available values are: 'tp' - Throughput sampler. 'cpu' - CPU Usage sampler
`sysTest.spiClass`|org.apache.activemq.tool.spi.ActiveMQClassLoaderSPI|The service provider interface class that allows the client to create a generic connection factory. Current available SPI classes include: 'org.apache.activemq.tool.ActiveMQPojoSPI' and 'org.apache.activemq.tool.ActiveMQClassLoaderSPI'

> **Note**: If the number of destinations is less than the number of clients and the distribution type is either 'equal' or 'divide', each client will send/receive from only one destination, distributing the destinations among the clients. i.e. if there are 5 producers and 2 destinations, 3 producers will send to destination 1, and 2 producers will send to destination 2. Also, a consumer can only receive from a single destination, unless composite destination is supported and specified.

### Configuration for running a JMS Producer

Configuration Key|Default Value|Description
---|---|---
`producer.sessTransacted`|false|Specifies if the session created will be transacted or not. See the JMS Specifications for more details.
`producer.sessAckMode`|autoAck|Specified the acknowledge mode of the session. See the JMS Specifications for more details. Available values are: 'autoAck' - Session.AUTO_ACKNOWLEDGE. 'clientAck' - Session.CLIENT_ACKNOWLEDGE. 'dupsAck' - Session.DUPS_OK_ACKNOWLEDGE. 'transacted' - Session.TRANSACTED
`producer.commitAfterXMsgs`|1|Commit transaction after the specified amount of messages. Only used in conjunction with a transacted JMS Session (producer.sessTransacted=true and producer.sessAckMode=transacted)
`producer.destName`|TEST.FOO|The prefix of the destination name to use. To specify a queue, prefix the destination name with 'queue://', for topics, prefix the destination with 'topic://'. If no prefix is specified, a topic will be created.
`producer.destComposite`|false|If there are more than one destination, and destComposite=true, the destinations will be merged into one. This assumes that the provider supports composite destinations.
`producer.deliveryMode`|nonpersistent|The message delivery mode that will be used for each producer. Available values are: 'nonpersistent' - use non-persistent delivery mode. javax.jms.DeliveryMode.NON_PERSISTENT. 'persistent' - use persistent delivery mode. javax.jms.DeliveryMode.PERSISTENT.
`producer.messageSize`|1024 bytes|The size of each text message to send.
`producer.msgFileName`|null|If you want to send a fixed message from a particular file. The file needs to be a text file. The entire content of the file will be sent as message body. Specifying a file containing the message to be sent makes the producer ignore the property producer.messageSize. Use relative or full path. Example: -Dproducer.msgFileName=../message.xml
`producer.createNewMsg`|false|If true, each send will create a new JMS Message with the specified message size with incrementing text messages (i.e. Text Message 1, Text Message 2, ...). If false, only one message will be created before the send loop, and this message will be sent over and over again.
`producer.sendDelay`|0 ms|Pause producer for sendDelay milliseconds after each message (allows producer throttling).
`producer.sendType`|time|Send either time-based or message-count-based. Available values are: 'time' - keep sending messages until a specific interval of time elapses. 'count' - keep sending messages until N messages has been sent.
`producer.sendCount`|1000000 msgs (1 million)|If sendType=count, send this number of messages.
`producer.sendDuration`|300000 ms (5 mins)|If sendType=time, send messages for this number of milliseconds.
`producer.header`|none|Specify message headers to be set on all messages. Headers use name=value syntax and can be separated using ':'. More than one header can be set on a single producer.header line, e.g. producer.header="JMSType=car:MyHeader=MyValue" (no spaces). Only one producer.header line is allowed!

### Configuration for running a JMS Consumer

Configuration Key|Default Value|Description
---|---|---
`consumer.sessTransacted`|false|Specifies if the session created will be transacted or not. See the JMS Specifications for more details.
`consumer.sessAckMode`|autoAck|Specified the acknowledge mode of the session. See the JMS Specifications for more details. Available values are: 'autoAck' - Session.AUTO_ACKNOWLEDGE. 'clientAck' - Session.CLIENT_ACKNOWLEDGE. 'dupsAck' - Session.DUPS\_OK\_ACKNOWLEDGE. 'transacted' - Session.TRANSACTED
`consumer.commitAfterXMsgs`|1|Commit transaction after the specified amount of messages. Only used in conjunction with a transacted JMS Session (consumer.sessTransacted=true and consumer.sessAckMode=transacted)
`consumer.destName`|TEST.FOO|The prefix of the destination name to use. To specify a queue, prefix the destination name with 'queue://', for topics, prefix the destination with 'topic://'. If no prefix is specified, a topic will be created.
`consumer.destComposite`|false|If there are more than one destination, and destComposite=true, the destinations will be merged into one. This assumes that the provider supports composite destinations.
`consumer.durable`|false|If true, create a durable subscriber, otherwise create a message consumer. See the JMS Specifications for more details.
`consumer.asyncRecv`|true|If true, asynchronously receive messages using the onMessage() method, otherwise use the receive() method.
`consumer.recvDelay`|0 ms|Pause consumer for recvDelay milliseconds with each message (allows consumer throttling).
`consumer.recvType`|time|Receive either time-based or message-count-based. Available values are: 'time' - keep receiving messages until a specific time interval has elapsed. 'count' - keep receiving until N messages has been received.
`consumer.recvCount`|1000000 msgs (1 million)|If recvType=count, receive this much messages.
`consumer.recvDuration`|300000 ms (5 mins)|If recvType=time, receive messages for this specific time duration.
`consumer.messageSelector`|none|Specify [message selector](selectors) for all consumers. E.g. consumer.messageSelector="MyHeader = 'MyValue' AND JMSType = 'car'"

> **Note**: If you have more than one consumer receiving from multiple destinations, it is a good idea to change the sysTest.destDistro to 'equal', since by default it uses 'all' and a consumer can only receive from a single destination, hence all consumers will receive from the first destination in the list only.

### Configuration for SPI Connection Factory: org.apache.activemq.tool.spi.ActiveMQPojoSPI

**Description:** This provides details in configuring the JMS Connection Factory created by ActiveMQPojoSPI. Default values are based from the default values of the service provider org.apache.activemq.ActiveMQConnectionFactory.

Configuration Key|Default Value|Description
---|---|---
`factory.brokerURL`|tcp://localhost:61616|The url of the broker the client will connect to.
`factory.userName`|null|Username on the connection to use.
`factory.password`|null|Password on the connection to use.
`factory.clientID`|null|Client ID the connection will use. If none is specified, it will be automatically generated.
`factory.asyncSend`|false|If true, asynchronously send messages.
`factory.asyncDispatch`|false|If true, asynchronously dispatch messages.
`factory.asyncSession`|true|If true, session will dispatch messages asynchronously.
`factory.closeTimeout`|15000 ms|
`factory.copyMsgOnSend`|true|If true, creates a copy of the message to be sent.
`factory.disableTimestamp`|false|If true, disable the setting of the JMSTimestamp.
`factory.deferObjSerial`|false|If true, defer the serialization of message objects.
`factory.optimAck`|true|If true, optimizes the acknowledgement of messages.
`factory.optimDispatch`|true|If true, optimizes the dispatching of messages.
`factory.prefetchQueue`|1000 messages|Number of messages a queue consumer will cache in RAM before processing it.
`factory.prefetchTopic`|32766 messages|Number of messages a topic consumer will cache in RAM before processing it.
`factory.useCompression`|false|If true, compress message data.
`factory.useRetroactive`|false|If true, make consumers retroactive.

### Configuration for SPI Connection Factory: `org.apache.activemq.tool.spi.ActiveMQClassLoaderSPI`

**Description:** This provides details in configuring the JMS Connection Factory created by `ActiveMQClassLoaderSPI`. Default values are based from the default values of the service provider org.apache.activemq.ActiveMQConnectionFactory.

ActiveMQClassLoaderSPI loads from the classpath `org.apache.activemq.ActiveMQConnectionFactory` and configures it using reflection. Configuration is generally based on the API of the class loaded. General format is `factory.YYY` or `factory.XXX.YYY`, where the last variable (`YYY`) is the property to set and everything in between is the getter of the class to use to set (`YYY`). For example:

1.  To set the value for asyncSend in ActiveMQConnectionFactory, use:  
    ```
    factory.useAsyncSend=true
    ```
    which is equivalent to calling 
    ```
    factory.setUseAsyncSend(true)
    ```

1.  To set the queue prefetch for ActiveMQConnectionFactory, use:  
    ```
    factory.prefetchPolicy.queuePrefetch=1
    ```
    which is equivalent to calling
    ```
    factory.getPrefetchPolicy().setQueuePrefetch(1)
    ```

It should be noted that the loaded class should implement the appropriate getter and setter methods. Nested objects should also be properly instantiated. For more information on configuring this SPI, refer to the specific provider API manual.

As of ActiveMQ 4.0.1, these are the available fields and default values:

Configuration Key|Default Value
---|---
`factory.prefetchPolicy.optimizeDurableTopicPrefetch`|1000
`factory.prefetchPolicy.durableTopicPrefetch`|100
`factory.password`|null
`factory.prefetchPolicy.queueBrowserPrefetch`|500
`factory.useCompression`|false
`factory.disableTimeStampsByDefault`|false
`factory.optimizedMessageDispatch`|true
`factory.useRetroactiveConsumer`|false
`factory.alwaysSessionAsync`|true
`factory.copyMessageOnSend`|true
`factory.prefetchPolicy.topicPrefetch`|32766
`factory.useAsyncSend`|false
`factory.redeliveryPolicy.backOffMultiplier`|5
`factory.prefetchPolicy.inputStreamPrefetch`|100
`factory.closeTimeout`|15000
`factory.userName`|null
`factory.optimizeAcknowledge`|false
`factory.clientID`|null
`factory.objectMessageSerializationDefered`|false
`factory.asyncDispatch`|false
`factory.redeliveryPolicy.initialRedeliveryDelay`|1000
`factory.prefetchPolicy.queuePrefetch`|1000
`factory.redeliveryPolicy.maximumRedeliveries`|5
`factory.redeliveryPolicy.useCollisionAvoidance`|false
`factory.prefetchPolicy.maximumPendingMessageLimit`|0
`factory.redeliveryPolicy.useExponentialBackOff`|false

### Configuration for samplers

There are two samplers built into the perfomance test tool. Both are configured with the same configuration keys, only prefixed with different sampler names.

Sampler|_samplerName_
Throughput Sampler|tpSampler
CPU Sampler|cpuSampler

For example, it is possible to configure the two samplers differently for a single test run, using the prefixes above as follows:
```
mvn activemq-perf:producer -DtpSampler.duration=10000 -DcpuSampler.duration=15000
```
By default, both samplers are used within a test run. This however, can be overridden through the use of the `sysTest.samplers` property.

#### Before ActiveMQ 5.11

Sampler configurations are not tied in to the test run, so it is possible for the sampler to run for longer than the actual test itself. For short run durations, the sampler may not even have ramped up before the test completes.

Configuration Key|Default Value|Description
---|---|---
`<samplerName>.duration`|300000|The total duration (in ms) the sampler will run, including ramp up and ramp down time.
`<samplerName>.rampUpTime`|30000|The ramp up time of the sampler, sampling will only start after the ramp up time.
`<samplerName>.rampDownTime`|30000|The ramp down time of the sampler, sampling will stop when the sampler has executed for (duration - rampUpTime - rampDownTime) ms.
`<samplerName>.interval`|1000|The interval (in ms), the sampler will sample for data.

#### From ActiveMQ 5.11

Sampler run duration is tied directly to the behavior of the client (producer/consumer). Due to the non-deterministic nature of count-based tests (where consumer.recvType or producer.sendType are set to `count`), duration, ramp up and ramp down times are ignored; the samplers are shut down when the client completes.

Configuration Key|Default Value|Description
---|---|---
`<samplerName>.duration`|For time-based tests, matches the consumer or producer duration. _Ignored for count-based tests._|The total duration (in ms) the sampler will run, including ramp up and ramp down time.
`<samplerName>.rampUpTime`|null; defers to rampUpPercent. _Ignored for count-based tests._|The ramp up time of the sampler, sampling will only start after the ramp up time.
`<samplerName>.rampDownTime`|null; defers to rampDownPercent. _Ignored for count-based tests._|The ramp down time of the sampler, sampling will stop when the sampler has executed for (duration - rampUpTime - rampDownTime) ms.
`<samplerName>.rampUpPercent`|0 _Ignored for count-based tests._|The percentage of the overall run duration that the sampler should use to ramp up (0-100). Overridden by rampUpTime.
`<samplerName>.rampDownPercent`|0 _Ignored for count-based tests._|The percentage of the overall run duration that the sampler should use to ramp down (0-99). Overridden by rampDownTime.
`<samplerName>.interval`|1000|The interval (in ms), the sampler will sample for data.

The time taken to ramp up and ramp down, whether defined through milliseconds or as a percentage cannot exceed the total duration.
