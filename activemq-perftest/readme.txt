####################################################################################################
# Running Maven 2 Performance Test
####################################################################################################
Goals                  | Description
-----------------------|----------------------------------------------------------
 activemq-perf:broker  | Starts broker using the activemq configuration file located in 
                       | "src\main\resources\broker-conf" where the default config is 
                       | activemq.xml.
                       |
                       | Parameters:
                       |   1. -DconfigType - specifies the type of configuration to use. 
                       |                     Its value must be one of the filename in the 
                       |                     "..\broker-config" directory (e.g. -DconfigType=kaha).
                       |   2. -DconfigFile - path to config file other than those in 
                       |                     "src\..\broker-config".
                       |                     (e.g -DconfigFile=c:\dir\activemq.xml)  
                       |
 activemq-perf:consumer| Starts the consumer's performance testing. 
                       | 
 activemq-perf:producer| Starts the producer's performance testing.
                       |
                       | * Note: The parameters for both consumer and producers are described on 
                       |   the next section. Also, the generated report is stored in the directory
                       |   specified in the parameter "sysTest.reportDirectory".
                       |

####################################################################################################
# Configuration for running a system of JMS Clients (Producer/Consumer)
####################################################################################################
Configuration Key        | Default Value    | Description
-------------------------|------------------|----------------------------------------------------------
 sysTest.numClients      | 1                | Specifies the number of JMS Clients to start.
                         |                  |
 sysTest.totalDests      | 1                | Specifies the total number of destinations to use for the
                         |                  | whole system test.
                         |                  |
 sysTest.destDistro      | all              | Specifies how to distribute the destinations to the
                         |                  | clients. Available values are (Invalid value will result
                         |                  | to using the default value 'all')*:
                         |                  | 'all' - All clients will send/receive to all destinations.
                         |                  |       i.e. if there are 2 producers and 5 destinations,
                         |                  |       each producer will send a message to each
                         |                  |       individual destination.
                         |                  | 'equal' - Clients will equally divide the destinations
                         |                  |         among themselves. i.e. if there are 2 producers and
                         |                  |         5 destinations, each producer will send messages to
                         |                  |         2 destinations. The fifth destination will not be
                         |                  |         used.
                         |                  | 'divide' - Clients will divide the destinations among 
                         |                  |          themselves regardless of equality. i.e. if there
                         |                  |          are 2 producers and 5 destinations, producer 1 will
                         |                  |          send to 3 destinations, while producer 2 will send
                         |                  |          to 2 destinations.
                         |                  |
 sysTest.reportDirectory | target/perf-test | The directory where the sampler report will be saved.
                         |                  |
                         
* Note: If the number of destinations is less than the number of clients and the distribution type is
  either 'equal' or 'divide', each client will send/receive from only one destination, distributing the 
  destinations among the clients. i.e. if there are 5 producers and 2 destinations, 3 producers will
  send to destination 1, and 2 producers will send to destination 2. Also, a consumer can only receive from
  a single destination, unless composite destination is supported and specified.
                         
####################################################################################################
# Configuration for running a JMS Producer
####################################################################################################
Configuration Key        | Default Value | Description
-------------------------|---------------|----------------------------------------------------------
 producer.spiClass       | null          | The service provider interface class that allows the
                         |               | client to create a generic connection factory. Current
                         |               | available SPI classes include:
                         |               |   1. 'org.apache.activemq.tool.ActiveMQPojoSPI'
                         |               |   2. 'org.apache.activemq.tool.ActiveMQClassLoaderSPI'
                         |               |
 producer.sessTransacted | false         | Specifies if the session created will be transacted or
                         |               | not. See the JMS Specifications for more details.
                         |               |
 producer.sessAckMode    | autoAck       | Specified the acknowledge mode of the session. See the
                         |               | JMS Specifications for more details. Available values are:
                         |               |  1. 'autoAck'    - Session.AUTO_ACKNOWLEDGE
                         |               |  2. 'clientAck'  - Session.CLIENT_ACKNOWLEDGE
                         |               |  3. 'dupsAck'    - Session.DUPS_OK_ACKNOWLEDGE
                         |               |  4. 'transacted' - Session.TRANSACTED
                         |               |
 producer.destName       | TEST.FOO      | The prefix of the destination name to use. To specify a
                         |               | queue, prefix the destination name with 'queue://', for 
                         |               | topics, prefix the destination with 'topic://'. If no 
                         |               | prefix is specified, a topic will be created.
                         |               |
 producer.destCount      | 1             | The number of destinations assigned to this client.*
                         |               |
 producer.destIndex      | 0             | The starting index of the destination name. i.e. if
                         |               | destName='TEST.FOO', destCount=3, destIndex=3, 3 topics
                         |               | will be created with names, TEST.FOO.3, TEST.FOO.4,
                         |               | TEST.FOO.5.*
                         |               |
 producer.destComposite  | false         | If there are more than one destination, and destComposite=
                         |               | true, the destinations will be merged into one. This
                         |               | assumes that the provider supports composite destinations.
                         |               |
 producer.messageSize    | 1024 bytes    | The size of each text message to send.
                         |               |
 producer.sendType       | time          | Send either time-based or message-count-based. Available
                         |               | values are:
                         |               |  1. 'time' - keep sending messages until a specific
                         |               |     interval of time elapses.
                         |               |  2. 'count' - keep sending messages until N messages has
                         |               |     been sent.
                         |               |
 producer.sendCount      | 1000000 msgs  | If sendType=count, send this number of messages.
                         | (1 million)   |
                         |               |
 producer.sendDuration   | 300000 ms     | If sendType=time, send messages for this number of
                         | (5 mins)      | milliseconds.
                         |               |

* Note: If you are using the Producer JMS System to create the producers, you need not bother with
  these settings, as this will be overwritten by the destination distribution method. i.e. if you
  specify 2 producers, 5 destinations, and equal distribution, producer 1 will have a destCount=2
  and a destIndex=0, while producer 2 will have a destCount=2, and a destIndex=2. Any previous value
  will be overwritten.

####################################################################################################
# Configuration for running a JMS Consumer
####################################################################################################
Configuration Key        | Default Value      | Description
-------------------------|--------------------|-----------------------------------------------------
 consumer.spiClass       | null               | The service provider interface class that allows the
                         |                    | client to create a generic connection factory.
                         |                    | Current available SPI classes include:
                         |                    |   1. 'org.apache.activemq.tool.ActiveMQPojoSPI'
                         |                    |   2. 'org.apache.activemq.tool.ActiveMQClassLoaderSPI'
                         |                    |
 consumer.sessTransacted | false              | Specifies if the session created will be transacted
                         |                    | or not. See the JMS Specifications for more details.
                         |                    |
 consumer.sessAckMode    | autoAck            | Specified the acknowledge mode of the session. See the
                         |                    | JMS Specifications for more details. Available values
                         |                    | are:
                         |                    |  1. 'autoAck'    - Session.AUTO_ACKNOWLEDGE
                         |                    |  2. 'clientAck'  - Session.CLIENT_ACKNOWLEDGE
                         |                    |  3. 'dupsAck'    - Session.DUPS_OK_ACKNOWLEDGE
                         |                    |  4. 'transacted' - Session.TRANSACTED
                         |                    |
 consumer.destName       | TEST.FOO           | The prefix of the destination name to use. To
                         |                    | specify a queue, prefix the destination name with
                         |                    | 'queue://', for topics, prefix the destination with 
                         |                    | 'topic://'. If no prefix is specified, a topic will
                         |                    | be created.
                         |                    |
 consumer.destCount      | 1                  | The number of destinations assigned to this client.*
                         |                    |
 consumer.destIndex      | 0                  | The starting index of the destination name. i.e. if
                         |                    | destName='TEST.FOO', destCount=3, destIndex=3, 
                         |                    | 3 topics will be created with names, TEST.FOO.3,
                         |                    | TEST.FOO.4, TEST.FOO.5.*
                         |                    |
 consumer.destComposite  | false              | If there are more than one destination, and 
                         |                    | destComposite=true, the destinations will be merged
                         |                    | into one. This assumes that the provider supports
                         |                    | composite destinations.
                         |                    |
 consumer.durable        | false              | If true, create a durable subscriber, otherwise
                         |                    | create a message consumer. See the JMS Specifications
                         |                    | for more details.
                         |                    |
 consumer.asyncRecv      | true               | If true, asynchronously receive messages using the
                         |                    | onMessage() method, otherwise use the receive()
                         |                    | method.
                         |                    |
 consumer.consumerName   | TestConsumerClient | Prefix that will be use for the subscription of the
                         |                    | consumer. Generally used for durable subscriber.
                         |                    |
 consumer.recvType       | time               | Receive either time-based or message-count-based.
                         |                    | Available values are:
                         |                    |  1. 'time' - keep receiving messages until a 
                         |                    |     specific time interval has elapsed.
                         |                    |  2. 'count' - keep receiving until N messages
                                              |     has been received.
                         |                    |
 consumer.recvCount      | 1000000 msgs       | If recvType=count, receive this much messages.
                         | (1 million)        |
                         |                    |
 consumer.recvType       | 300000 ms          | If recvType=time, receive messages for this specific
                         | (5 mins)           | time duration.
                         |                    |
  
* Note: If you are using the Consumer JMS System to create the consumers, you need not bother with
  these settings, as this will be overwritten by the destination distribution method. i.e. if you
  specify 2 consumer, 5 destinations, and equal distribution, consumer 1 will have a destCount=2
  and a destIndex=0, while consumer 2 will have a destCount=2, and a destIndex=2. Any previous value
  will be overwritten. Although, it should be noted that unless composite destination is supported
  and specified, each consumer will choose only one destination to receive from.

####################################################################################################
# Configuration for SPI Connection Factory: org.apache.activemq.tool.spi.ActiveMQPojoSPI
# Description: This provides details in configuring the JMS Connection Factory created by
#              ActiveMQPojoSPI. Default values are based from the default values of the service
#              provider org.apache.activemq.ActiveMQConnectionFactory.
####################################################################################################

Configuration Key         | Default Value         | Description
--------------------------|-----------------------|--------------------------------------------------
 factory.brokerUrl        | tcp://localhost:61616 | The url of the broker the client will connect to.
                          |                       |
 factory.username         | null                  | Username on the connection to use.
                          |                       |
 factory.password         | null                  | Password on the connection to use.
                          |                       |
 factory.clientID         | null                  | Client ID the connection will use. If none is
                          |                       | specified, it will be automatically generated.
                          |                       |
 factory.asyncSend        | false                 | If true, asynchronously send messages
                          |                       |
 factory.asyncDispatch    | false                 | If true, asynchronously dispatch messages
                          |                       |
 factory.asyncSession     | true                  | If true, session will dispatch messages
                          |                       | asynchronously.
                          |                       |
 factory.closeTimeout     | 15000 ms              | 
                          |                       |
 factory.copyMsgOnSend    | true                  | If true, creates a copy of the message to be sent
                          |                       |
 factory.disableTimestamp | false                 | If true, disable the setting of the JMSTimestamp.
                          |                       |
 factory.deferObjSerial   | false                 | If true, defer the serialization of message objects.
                          |                       |
 factory.onSendPrepMsg    | true                  | If true, prepare a message before sending it.
                          |                       |
 factory.optimAck         | true                  | If true, optimizes the acknowledgement of messages.
                          |                       |
 factory.optimDispatch    | true                  | If true, optimizes the dispatching of messages.
                          |                       |
 factory.prefetchQueue    | 1000 messages         | Number of messages a queue consumer will cache 
                          |                       | in RAM before processing it. 
                          |                       |
 factory.prefetchTopic    | 32766 messages        | Number of messages a topic consumer will cache
                          |                       | in RAM before processing it.
                          |                       |
 factory.useCompression   | false                 | If true, compress message data.
                          |                       |
 factory.useRetroactive   | false                 | If true, make consumers retroactive.
                          |                       |
                          
                          
####################################################################################################
# Configuration for SPI Connection Factory: org.apache.activemq.tool.spi.ActiveMQClassLoaderSPI
# Description: This provides details in configuring the JMS Connection Factory created by
#              ActiveMQClassLoaderSPI. Default values are based from the default values of the
#              service provider org.apache.activemq.ActiveMQConnectionFactory.
####################################################################################################

ActiveMQClassLoaderSPI loads from the classpath "org.apache.activemq.ActiveMQConnectionFactory" and
configures it using reflection. Configuration is generally based on the API of the class loaded.
General format is factory.YYY or factory.XXX.YYY, where the last variable (YYY) is the property to
set and everything in between is the getter of the class to use to set (YYY). For example:

1. To set the value for asyncSend in ActiveMQConnectionFactory, use:
   factory.useAsyncSend=true, which is equivalent to calling factory.setUseAsyncSend(true);

2. To set the queue prefetch for ActiveMQConnectionFactory, use:
   factory.prefetchPolicy.queuePrefetch=1, which is equivalent to calling
   factory.getPrefetchPolicy().setQueuePrefetch(1);

It should be noted that the loaded class should implement the appropriate getter and setter methods.
Nested objects should also be properly instantiated. For more information on configuring this SPI,
refer to the specific provider API manual.