#include "DurableTester.h"
#include <integration/common/IntegrationCommon.h>

CPPUNIT_TEST_SUITE_REGISTRATION( integration::durable::DurableTester );

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
using namespace integration::durable;
using namespace integration::common;

DurableTester::DurableTester() : AbstractTester()
{}

DurableTester::~DurableTester()
{}

void DurableTester::test()
{
    try
    {
        cout << "Starting activemqcms durable test (sending "
             << IntegrationCommon::defaultMsgCount
             << " messages per type and sleeping "
             << IntegrationCommon::defaultDelay 
             << " milli-seconds) ...\n"
             << endl;
        
        std::string subName = Guid().createGUID();

        // Create CMS Object for Comms
        cms::Topic* topic = session->createTopic("mytopic");
        cms::MessageConsumer* consumer = 
            session->createDurableConsumer( *topic, subName, "" );            
        consumer->setMessageListener( this );
        cms::MessageProducer* producer = 
            session->createProducer( *topic );

        unsigned int sent;

        // Send some text messages
        sent = this->produceTextMessages( *producer, 3 );
        
        // Wait for all messages
        waitForMessages( sent );

        printf("received: %d\n", numReceived );
        CPPUNIT_ASSERT( numReceived == sent );

        // Nuke the consumer
        delete consumer;

        // Send some text messages
        sent += this->produceTextMessages( *producer, 3 );

        consumer = session->createDurableConsumer( *topic, subName, "" );            

        // Send some text messages
        sent += this->produceTextMessages( *producer, 3 );

        // Wait for all remaining messages
        waitForMessages( sent - numReceived );
        
        printf("received: %d\n", numReceived );
 //       CPPUNIT_ASSERT( numReceived == sent );

        printf("Shutting Down\n" );
        delete producer;                      
        delete consumer;
    }
    AMQ_CATCH_RETHROW( ActiveMQException )
    AMQ_CATCHALL_THROW( ActiveMQException )
}
