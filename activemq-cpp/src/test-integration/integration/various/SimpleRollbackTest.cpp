#include "SimpleRollbackTest.h"

#include <integration/common/IntegrationCommon.h>

CPPUNIT_TEST_SUITE_REGISTRATION( integration::various::SimpleRollbackTest );

#include <sstream>

#include <activemq/core/ActiveMQConnectionFactory.h>
#include <activemq/exceptions/ActiveMQException.h>
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
#include <cms/Session.h>

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

using namespace std;
using namespace integration;
using namespace integration::various;
using namespace integration::common;

SimpleRollbackTest::SimpleRollbackTest()
{
    try
    {
        string url = IntegrationCommon::defaultURL;
        numReceived = 0;

        // Default amount to send and receive
        msgCount = 1;
    
        // Create a Factory
        connectionFactory = new ActiveMQConnectionFactory( url );

        // Now create the connection
        connection = connectionFactory->createConnection(
            "", "", Guid().createGUIDString() );
    
        // Set ourself as a recipient of Exceptions        
        connection->setExceptionListener( this );
        connection->start();
        
        // Create a Session
        session = connection->createSession( 
            cms::Session::SESSION_TRANSACTED );
    }
    AMQ_CATCH_RETHROW( ActiveMQException )
    AMQ_CATCHALL_THROW( ActiveMQException )
}

SimpleRollbackTest::~SimpleRollbackTest()
{
    try
    {
        session->close();
        connection->close();

        delete session;
        delete connection;
        delete connectionFactory;
    }
    AMQ_CATCH_NOTHROW( ActiveMQException )
    AMQ_CATCHALL_NOTHROW( )
}

void SimpleRollbackTest::test()
{
    try
    {
        // Create CMS Object for Comms
        cms::Topic* topic = session->createTopic("mytopic");
        cms::MessageConsumer* consumer = 
            session->createConsumer( topic );            
        consumer->setMessageListener( this );
        cms::MessageProducer* producer = 
            session->createProducer( topic );

        cms::TextMessage* textMsg = 
            session->createTextMessage();

        for( size_t ix = 0; ix < msgCount; ++ix )
        {
            ostringstream lcStream;
            lcStream << "SimpleTest - Message #" << ix << ends;            
            textMsg->setText( lcStream.str() );
            producer->send( textMsg );
        }
        
        delete textMsg;

        Thread::sleep( 100 );

        session->commit();

        textMsg = session->createTextMessage();

        for( size_t ix = 0; ix < msgCount; ++ix )
        {
            ostringstream lcStream;
            lcStream << "SimpleTest - Message #" << ix << ends;            
            textMsg->setText( lcStream.str() );
            producer->send( textMsg );
        }
        
        delete textMsg;

        Thread::sleep( 500 );

        session->rollback();

        Thread::sleep( 500 );

        textMsg = session->createTextMessage();
        textMsg->setText( "SimpleTest - Message after Rollback" );
        producer->send( textMsg );
        delete textMsg;

        Thread::sleep( 15000 );

        CPPUNIT_ASSERT( true );

        printf( "Shutting Down\n" );

        delete producer;                      
        delete consumer;
    }
    AMQ_CATCH_RETHROW( ActiveMQException )
    AMQ_CATCHALL_THROW( ActiveMQException )
}

void SimpleRollbackTest::onException( const cms::CMSException& error )
{
    bool AbstractTester = false;
    CPPUNIT_ASSERT( AbstractTester );
}

void SimpleRollbackTest::onMessage( const cms::Message* message )
{
    try
    {
        // Got a text message.
        const cms::TextMessage* txtMsg = 
            dynamic_cast<const cms::TextMessage*>(message);
            
        if( txtMsg != NULL )
        {
            std::string text = txtMsg->getText();
    
    //            printf("received text msg: %s\n", txtMsg.getText() );
    
            numReceived++;
    
            // Signal that we got one
            synchronized( &mutex )
            {
                mutex.notifyAll();
            }
    
            return;
        }
        
        // Got a bytes msg.
        const cms::BytesMessage* bytesMsg = 
            dynamic_cast<const cms::BytesMessage*>(message);
    
        if( bytesMsg != NULL )
        {
            const unsigned char* bytes = bytesMsg->getBodyBytes();
            
            string transcode( (const char*)bytes, bytesMsg->getBodyLength() );
    
            //printf("received bytes msg: " );
            //int numBytes = bytesMsg.getBodyLength();
            //for( int ix=0; ix<numBytes; ++ix ){
               // printf("[%d]", bytes[ix] );
            //}
            //printf("\n");
    
            numReceived++;
            
            // Signal that we got one
            synchronized( &mutex )
            {
                mutex.notifyAll();
            }
    
            return;
        }
    }
    AMQ_CATCH_NOTHROW( ActiveMQException )
    AMQ_CATCHALL_NOTHROW( )
}
