#include "SimpleTester.h"
#include <integration/common/IntegrationCommon.h>

CPPUNIT_TEST_SUITE_REGISTRATION( integration::simple::SimpleTester );

#include <activemq/concurrent/Thread.h>
#include <activemq/connector/stomp/StompConnector.h>
#include <activemq/util/SimpleProperties.h>
#include <activemq/transport/TransportFactory.h>
#include <activemq/util/Guid.h>
#include <activemq/util/SimpleProperties.h>
#include <activemq/util/StringTokenizer.h>
#include <activemq/connector/ConnectorFactoryMap.h>
#include <activemq/network/SocketFactory.h>
#include <activemq/transport/TransportFactory.h>
#include <activemq/network/Socket.h>
#include <activemq/exceptions/NullPointerException.h>
#include <activemq/core/ActiveMQConnection.h>
#include <activemq/core/ActiveMQConsumer.h>
#include <activemq/core/ActiveMQProducer.h>
#include <activemq/util/StringTokenizer.h>
#include <activemq/util/Boolean.h>

#include <cms/Connection.h>
#include <cms/MessageConsumer.h>
#include <cms/MessageProducer.h>
#include <cms/MessageListener.h>
#include <cms/Startable.h>
#include <cms/Closeable.h>
#include <cms/MessageListener.h>
#include <cms/ExceptionListener.h>
#include <cms/Topic.h>
#include <cms/Queue.h>
#include <cms/TemporaryTopic.h>
#include <cms/TemporaryQueue.h>
#include <cms/Session.h>
#include <cms/BytesMessage.h>
#include <cms/TextMessage.h>
#include <cms/MapMessage.h>

using namespace activemq::connector::stomp;
using namespace activemq::transport;
using namespace activemq::util;
using namespace std;
using namespace cms;
using namespace activemq;
using namespace activemq::core;
using namespace activemq::util;
using namespace activemq::connector;
using namespace activemq::exceptions;
using namespace activemq::network;
using namespace activemq::transport;
using namespace activemq::concurrent;

using namespace integration;
using namespace integration::simple;
using namespace integration::common;

SimpleTester::SimpleTester() : AbstractTester()
{
    numReceived = 0;
}

SimpleTester::~SimpleTester()
{
}

void SimpleTester::test()
{
    try
    {
        cout << "Starting activemqcms test (sending "
             << IntegrationCommon::defaultMsgCount
             << " messages per type and sleeping "
             << IntegrationCommon::defaultDelay 
             << " milli-seconds) ...\n"
             << endl;
        
        // Create CMS Object for Comms
        cms::Topic* topic = session->createTopic("mytopic");
        cms::MessageConsumer* consumer = 
            session->createConsumer( *topic );            
        consumer->setMessageListener( this );
        cms::MessageProducer* producer = 
            session->createProducer( *topic );

        // Send some text messages
        this->produceTextMessages( 
            *producer, IntegrationCommon::defaultMsgCount );
        
        // Send some bytes messages.
        this->produceTextMessages( 
            *producer, IntegrationCommon::defaultMsgCount );

        // Wait for the messages to get here
        waitForMessages( IntegrationCommon::defaultMsgCount * 2 );
        
        printf("received: %d\n", numReceived );
        CPPUNIT_ASSERT( 
            numReceived == IntegrationCommon::defaultMsgCount * 2 );

        printf("Shutting Down\n" );
        delete producer;                      
        delete consumer;
    }
    AMQ_CATCH_RETHROW( ActiveMQException )
    AMQ_CATCHALL_THROW( ActiveMQException )
}

