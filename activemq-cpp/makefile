SRCDIR = src
OUTDIR = out
MAKESUPPORT_HOME = $(PWD)

OFILES = \
	$(OUTDIR)/main/activemq/exceptions/ActiveMQException.o \
	\
	$(OUTDIR)/main/activemq/support/InitDirector.o \
	\
	$(OUTDIR)/main/activemq/transport/IOTransport.o \
	$(OUTDIR)/main/activemq/transport/TcpTransport.o \
	$(OUTDIR)/main/activemq/transport/ResponseCorrelator.o \
	$(OUTDIR)/main/activemq/transport/TransportFactoryMap.o \
	$(OUTDIR)/main/activemq/transport/IOTransportFactory.o \
	$(OUTDIR)/main/activemq/transport/TcpTransportFactory.o \
	\
	$(OUTDIR)/main/activemq/connector/ConnectorFactoryMap.o \
	\
	$(OUTDIR)/main/activemq/connector/stomp/StompConnector.o \
	$(OUTDIR)/main/activemq/connector/stomp/StompConnectorFactory.o \
	$(OUTDIR)/main/activemq/connector/stomp/StompCommandReader.o \
	$(OUTDIR)/main/activemq/connector/stomp/StompCommandWriter.o \
	$(OUTDIR)/main/activemq/connector/stomp/StompSessionManager.o \
    \
	$(OUTDIR)/main/activemq/connector/stomp/commands/CommandConstants.o \
    \
	$(OUTDIR)/main/activemq/connector/stomp/marshal/Marshaler.o \
    \
	$(OUTDIR)/main/activemq/core/ActiveMQConnectionFactory.o \
	$(OUTDIR)/main/activemq/core/ActiveMQConnection.o \
	$(OUTDIR)/main/activemq/core/ActiveMQSession.o \
	$(OUTDIR)/main/activemq/core/ActiveMQProducer.o \
	$(OUTDIR)/main/activemq/core/ActiveMQConsumer.o \
	$(OUTDIR)/main/activemq/core/ActiveMQTransaction.o \
	$(OUTDIR)/main/activemq/core/ActiveMQConstants.o \
	\
	$(OUTDIR)/main/activemq/io/EndianReader.o \
	$(OUTDIR)/main/activemq/io/EndianWriter.o \
	$(OUTDIR)/main/activemq/io/BufferedInputStream.o \
	$(OUTDIR)/main/activemq/io/BufferedOutputStream.o \
	$(OUTDIR)/main/activemq/io/ByteArrayInputStream.o \
	$(OUTDIR)/main/activemq/io/ByteArrayOutputStream.o \
    \
	$(OUTDIR)/main/activemq/logger/SimpleLogger.o \
	$(OUTDIR)/main/activemq/logger/LogWriter.o \
	$(OUTDIR)/main/activemq/logger/LogManager.o \
	$(OUTDIR)/main/activemq/logger/LoggerHierarchy.o \
    \
    $(OUTDIR)/main/activemq/network/ServerSocket.o \
    $(OUTDIR)/main/activemq/network/TcpSocket.o \
    $(OUTDIR)/main/activemq/network/BufferedSocket.o \
    $(OUTDIR)/main/activemq/network/SocketFactory.o \
    $(OUTDIR)/main/activemq/network/SocketInputStream.o \
    $(OUTDIR)/main/activemq/network/SocketOutputStream.o\
    \
    $(OUTDIR)/main/activemq/util/Guid.o \
    $(OUTDIR)/main/activemq/util/StringTokenizer.o \
    \
    $(OUTDIR)/main/activemq/concurrent/Thread.o \
    $(OUTDIR)/main/activemq/concurrent/Mutex.o \
    $(OUTDIR)/main/activemq/concurrent/ThreadPool.o \
    $(OUTDIR)/main/activemq/concurrent/PooledThread.o 

OTESTFILES = \
    $(OUTDIR)/test/main.o \
    \
    $(OUTDIR)/test/activemq/core/ActiveMQDestinationTest.o \
    $(OUTDIR)/test/activemq/core/ActiveMQConnectionFactoryTest.o \
    $(OUTDIR)/test/activemq/core/ActiveMQConnectionTest.o \
    $(OUTDIR)/test/activemq/core/ActiveMQSessionTest.o \
    \
    $(OUTDIR)/test/activemq/concurrent/MutexTest.o \
    $(OUTDIR)/test/activemq/concurrent/ThreadPoolTest.o \
    $(OUTDIR)/test/activemq/concurrent/ThreadTest.o \
    \
    $(OUTDIR)/test/activemq/connector/stomp/StompConnectorTest.o \
    $(OUTDIR)/test/activemq/connector/stomp/StompFrameTest.o \
    $(OUTDIR)/test/activemq/connector/stomp/StompCommandReaderTest.o \
    $(OUTDIR)/test/activemq/connector/stomp/StompCommandWriterTest.o \
    $(OUTDIR)/test/activemq/connector/stomp/StompSessionManagerTest.o \
    \
    $(OUTDIR)/test/activemq/connector/stomp/commands/CommandConstantsTest.o \
	$(OUTDIR)/test/activemq/connector/stomp/commands/AbortCommandTest.o \
	$(OUTDIR)/test/activemq/connector/stomp/commands/AckCommandTest.o \
	$(OUTDIR)/test/activemq/connector/stomp/commands/BeginCommandTest.o \
	$(OUTDIR)/test/activemq/connector/stomp/commands/CommitCommandTest.o \
	$(OUTDIR)/test/activemq/connector/stomp/commands/ConnectCommandTest.o \
	$(OUTDIR)/test/activemq/connector/stomp/commands/ConnectedCommandTest.o \
	$(OUTDIR)/test/activemq/connector/stomp/commands/DisconnectCommandTest.o \
	$(OUTDIR)/test/activemq/connector/stomp/commands/ErrorCommandTest.o \
	$(OUTDIR)/test/activemq/connector/stomp/commands/ReceiptCommandTest.o \
	$(OUTDIR)/test/activemq/connector/stomp/commands/SubscribeCommandTest.o \
	$(OUTDIR)/test/activemq/connector/stomp/commands/UnsubscribeCommandTest.o \
	$(OUTDIR)/test/activemq/connector/stomp/commands/MessageCommandTest.o \
	$(OUTDIR)/test/activemq/connector/stomp/commands/BytesMessageCommandTest.o \
	$(OUTDIR)/test/activemq/connector/stomp/commands/TextMessageCommandTest.o \
    \
	$(OUTDIR)/test/activemq/connector/stomp/marshal/MarshalerTest.o \
    \
    $(OUTDIR)/test/activemq/connector/ConnectorFactoryMapRegistrarTest.o \
    $(OUTDIR)/test/activemq/connector/ConnectorFactoryMapTest.o \
    \
    $(OUTDIR)/test/activemq/exceptions/ActiveMQExceptionTest.o \
    \
    $(OUTDIR)/test/activemq/io/BufferedInputStreamTest.o \
	$(OUTDIR)/test/activemq/io/BufferedOutputStreamTest.o \
	$(OUTDIR)/test/activemq/io/ByteArrayInputStreamTest.o \
	$(OUTDIR)/test/activemq/io/ByteArrayOutputStreamTest.o \
	$(OUTDIR)/test/activemq/io/EndianReaderTest.o \
	$(OUTDIR)/test/activemq/io/EndianWriterTest.o \
	\
	$(OUTDIR)/test/activemq/logger/LoggerTest.o \
	\
	$(OUTDIR)/test/activemq/network/SocketFactoryTest.o \
	$(OUTDIR)/test/activemq/network/SocketTest.o \
	\
	$(OUTDIR)/test/activemq/transport/DummyTransportFactory.o \
	$(OUTDIR)/test/activemq/transport/IOTransportTest.o \
	$(OUTDIR)/test/activemq/transport/ResponseCorrelatorTest.o \
	$(OUTDIR)/test/activemq/transport/TransportFactoryMapTest.o \
	$(OUTDIR)/test/activemq/transport/TransportFactoryMapRegistrarTest.o \
	\
	$(OUTDIR)/test/activemq/util/GuidTest.o \
	$(OUTDIR)/test/activemq/util/IntegerTest.o \
	$(OUTDIR)/test/activemq/util/LongTest.o \
	$(OUTDIR)/test/activemq/util/BooleanTest.o \
	$(OUTDIR)/test/activemq/util/QueueTest.o \
	$(OUTDIR)/test/activemq/util/StringTokenizerTest.o

OINTEGRATIONFILES = \
	$(OUTDIR)/test-integration/main.o \
	\
	$(OUTDIR)/test-integration/integration/simple/SimpleTester.o \
	$(OUTDIR)/test-integration/integration/transactional/TransactionTester.o \
	$(OUTDIR)/test-integration/integration/common/AbstractTester.o \
	$(OUTDIR)/test-integration/integration/common/IntegrationCommon.o \
	$(OUTDIR)/test-integration/integration/various/SimpleRollbackTest.o
	

# Increment this to get a build specific library.
VERSION = 0_0_2

LIBRARY_NAME   = activemq-cpp-$(VERSION)
LIBFILE        = $(OUTDIR)/lib$(LIBRARY_NAME).a
TESTEXE        = $(OUTDIR)/activemqTest
INTEGRATIONEXE = $(OUTDIR)/activemqIntegrationTests

DEFINES          =

include $(MAKESUPPORT_HOME)/makefile.cfg

