SRCDIR = src/main/cpp
MAKESUPPORT_HOME = $(PWD)

LIBOFILES = \
	$(OUTDIR)/activemq/BrokerException.o \
	$(OUTDIR)/activemq/Connection.o \
	$(OUTDIR)/activemq/ConnectionClosedException.o \
	$(OUTDIR)/activemq/ConnectionFactory.o \
	$(OUTDIR)/activemq/ConsumerClosedException.o \
	$(OUTDIR)/activemq/DestinationFilter.o \
	$(OUTDIR)/activemq/Dispatcher.o \
	$(OUTDIR)/activemq/MessageConsumer.o \
	$(OUTDIR)/activemq/MessageConsumerSynchronization.o \
	$(OUTDIR)/activemq/MessageProducer.o \
	$(OUTDIR)/activemq/Session.o \
	$(OUTDIR)/activemq/TransactionContext.o \
	\
	$(OUTDIR)/activemq/command/AbstractCommand.o \
	$(OUTDIR)/activemq/command/ActiveMQBytesMessage.o \
	$(OUTDIR)/activemq/command/ActiveMQDestination.o \
	$(OUTDIR)/activemq/command/ActiveMQMapMessage.o \
	$(OUTDIR)/activemq/command/ActiveMQMessage.o \
	$(OUTDIR)/activemq/command/ActiveMQObjectMessage.o \
	$(OUTDIR)/activemq/command/ActiveMQQueue.o \
	$(OUTDIR)/activemq/command/ActiveMQStreamMessage.o \
	$(OUTDIR)/activemq/command/ActiveMQTempQueue.o \
	$(OUTDIR)/activemq/command/ActiveMQTempTopic.o \
	$(OUTDIR)/activemq/command/ActiveMQTextMessage.o \
	$(OUTDIR)/activemq/command/ActiveMQTopic.o \
	$(OUTDIR)/activemq/command/BaseCommand.o \
	$(OUTDIR)/activemq/command/BrokerError.o \
	$(OUTDIR)/activemq/command/BrokerId.o \
	$(OUTDIR)/activemq/command/BrokerInfo.o \
	$(OUTDIR)/activemq/command/ConnectionError.o \
	$(OUTDIR)/activemq/command/ConnectionId.o \
	$(OUTDIR)/activemq/command/ConnectionInfo.o \
	$(OUTDIR)/activemq/command/ConsumerId.o \
	$(OUTDIR)/activemq/command/ConsumerInfo.o \
	$(OUTDIR)/activemq/command/ControlCommand.o \
	$(OUTDIR)/activemq/command/DataArrayResponse.o \
	$(OUTDIR)/activemq/command/DataResponse.o \
	$(OUTDIR)/activemq/command/DestinationInfo.o \
	$(OUTDIR)/activemq/command/DiscoveryEvent.o \
	$(OUTDIR)/activemq/command/ExceptionResponse.o \
	$(OUTDIR)/activemq/command/FlushCommand.o \
	$(OUTDIR)/activemq/command/IntegerResponse.o \
	$(OUTDIR)/activemq/command/JournalQueueAck.o \
	$(OUTDIR)/activemq/command/JournalTopicAck.o \
	$(OUTDIR)/activemq/command/JournalTrace.o \
	$(OUTDIR)/activemq/command/JournalTransaction.o \
	$(OUTDIR)/activemq/command/KeepAliveInfo.o \
	$(OUTDIR)/activemq/command/LocalTransactionId.o \
	$(OUTDIR)/activemq/command/Message.o \
	$(OUTDIR)/activemq/command/MessageAck.o \
	$(OUTDIR)/activemq/command/MessageDispatch.o \
	$(OUTDIR)/activemq/command/MessageDispatchNotification.o \
	$(OUTDIR)/activemq/command/MessageId.o \
	$(OUTDIR)/activemq/command/ProducerId.o \
	$(OUTDIR)/activemq/command/ProducerInfo.o \
	$(OUTDIR)/activemq/command/RemoveInfo.o \
	$(OUTDIR)/activemq/command/RemoveSubscriptionInfo.o \
	$(OUTDIR)/activemq/command/Response.o \
	$(OUTDIR)/activemq/command/SessionId.o \
	$(OUTDIR)/activemq/command/SessionInfo.o \
	$(OUTDIR)/activemq/command/ShutdownInfo.o \
	$(OUTDIR)/activemq/command/SubscriptionInfo.o \
	$(OUTDIR)/activemq/command/TransactionId.o \
	$(OUTDIR)/activemq/command/TransactionInfo.o \
	$(OUTDIR)/activemq/command/WireFormatInfo.o \
	$(OUTDIR)/activemq/command/XATransactionId.o \
	$(OUTDIR)/activemq/command/ActiveMQTempDestination.o \
	\
	$(OUTDIR)/activemq/protocol/openwire/OpenWireMarshaller.o \
	$(OUTDIR)/activemq/protocol/openwire/OpenWireProtocol.o \
	\
	$(OUTDIR)/activemq/transport/FutureResponse.o \
	$(OUTDIR)/activemq/transport/CorrelatorFilter.o \
	$(OUTDIR)/activemq/transport/LoggingFilter.o \
	$(OUTDIR)/activemq/transport/MutexFilter.o \
	$(OUTDIR)/activemq/transport/TransportFactory.o \
	$(OUTDIR)/activemq/transport/TransportFilter.o \
	\
	$(OUTDIR)/activemq/transport/tcp/TcpTransport.o \
	\
	$(OUTDIR)/cms/CmsException.o \
	\
	$(OUTDIR)/ppr/io/ByteArrayInputStream.o \
	$(OUTDIR)/ppr/io/ByteArrayOutputStream.o \
	$(OUTDIR)/ppr/io/DataInputStream.o \
	$(OUTDIR)/ppr/io/DataOutputStream.o \
	$(OUTDIR)/ppr/io/SocketInputStream.o \
	$(OUTDIR)/ppr/io/SocketOutputStream.o \
	\
	$(OUTDIR)/ppr/net/Socket.o \
	$(OUTDIR)/ppr/net/ServerSocket.o \
	$(OUTDIR)/ppr/net/Uri.o \
	\
	$(OUTDIR)/ppr/thread/Thread.o \
	\
	$(OUTDIR)/ppr/TraceException.o \
	$(OUTDIR)/ppr/util/Guid.o \
	$(OUTDIR)/ppr/util/Hex.o \
	$(OUTDIR)/ppr/util/MapItemHolder.o \
	$(OUTDIR)/ppr/util/Time.o \

OUTLIB = $(OUTDIR)/libactivemq.a

EXEOFILES = \
	$(OUTDIR)/TestMain.o \
	$(OUTDIR)/TestListener.o \

EXELIBS = -lactivemq -lpthread -lstdc++

DEFINES          =

include $(MAKESUPPORT_HOME)/makefile.cfg

