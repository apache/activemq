#ifndef _ACTIVEMQ_CONNECTOR_STOMP_STOMPCONNECTORTEST_H_
#define _ACTIVEMQ_CONNECTOR_STOMP_STOMPCONNECTORTEST_H_

#include <cppunit/TestFixture.h>
#include <cppunit/extensions/HelperMacros.h>

#include <activemq/connector/stomp/StompResponseBuilder.h>
#include <activemq/connector/stomp/StompConnector.h>
#include <activemq/transport/Command.h>
#include <activemq/connector/stomp/commands/ConnectedCommand.h>
#include <activemq/connector/stomp/commands/TextMessageCommand.h>
#include <activemq/connector/stomp/commands/BytesMessageCommand.h>
#include <activemq/connector/stomp/StompTopic.h>
#include <activemq/connector/stomp/StompQueue.h>
#include <activemq/transport/DummyTransport.h>

#include <activemq/io/ByteArrayOutputStream.h>
#include <algorithm>

namespace activemq{
namespace connector{
namespace stomp{

    class StompConnectorTest : public CppUnit::TestFixture
    {
        CPPUNIT_TEST_SUITE( StompConnectorTest );
        CPPUNIT_TEST( testSessions );
        CPPUNIT_TEST( testConsumers );
        CPPUNIT_TEST( testProducers );
        CPPUNIT_TEST( testCommand );
        CPPUNIT_TEST( testSendingCommands );
        CPPUNIT_TEST_SUITE_END();

    public:
    
    	StompConnectorTest() {}
    	virtual ~StompConnectorTest() {}
        
        class MyCommandListener : public transport::CommandListener{
        public:
        
            transport::Command* cmd;
            
        public:
        
            MyCommandListener(){
                cmd = NULL;
            }
            virtual ~MyCommandListener(){}
            
            virtual void onCommand( transport::Command* command ){
                cmd = command;
            }
        };
        
        class MyMessageListener : public ConsumerMessageListener{
        public:
        
            std::vector<ConsumerInfo*> consumers;
            
        public:
            virtual ~MyMessageListener(){}
            
            virtual void onConsumerMessage( ConsumerInfo* consumer,
                core::ActiveMQMessage* msg )
            {
                consumers.push_back( consumer );
            }
        };

        void testSessions()
        {
            std::string connectionId = "testConnectionId";
            StompResponseBuilder responseBuilder("testConnectionId");
            transport::DummyTransport transport( &responseBuilder );
            util::SimpleProperties properties;
            StompConnector connector( &transport, properties );
            connector.start();
            
            SessionInfo* info1 = connector.createSession( cms::Session::AutoAcknowledge );
            CPPUNIT_ASSERT( info1->getAckMode() == cms::Session::AutoAcknowledge );
            CPPUNIT_ASSERT( info1->getConnectionId() == connectionId );
            
            SessionInfo* info2 = connector.createSession( cms::Session::DupsOkAcknowledge );
            CPPUNIT_ASSERT( info2->getAckMode() == cms::Session::DupsOkAcknowledge );
            CPPUNIT_ASSERT( info2->getConnectionId() == connectionId );
            
            SessionInfo* info3 = connector.createSession( cms::Session::ClientAcknowledge );
            CPPUNIT_ASSERT( info3->getAckMode() == cms::Session::ClientAcknowledge );
            CPPUNIT_ASSERT( info3->getConnectionId() == connectionId );
            
            SessionInfo* info4 = connector.createSession( cms::Session::Transactional );
            CPPUNIT_ASSERT( info4->getAckMode() == cms::Session::Transactional );
            CPPUNIT_ASSERT( info4->getConnectionId() == connectionId );
            
            connector.destroyResource( info1 );
            connector.destroyResource( info2 );
            connector.destroyResource( info3 );
            connector.destroyResource( info4 );

        }
        
        void testConsumers()
        {
            std::string connectionId = "testConnectionId";
            StompResponseBuilder responseBuilder("testConnectionId");
            transport::DummyTransport transport( &responseBuilder );
            util::SimpleProperties properties;
            StompConnector connector( &transport, properties );
            connector.start();
            
            SessionInfo* info1 = connector.createSession( cms::Session::AutoAcknowledge );
            std::string sel1 = "";
            StompTopic dest1( "dummy.topic.1" );
            ConsumerInfo* cinfo1 = connector.createConsumer( &dest1, info1, sel1 );
            CPPUNIT_ASSERT( cinfo1->getSessionInfo() == info1 );
            CPPUNIT_ASSERT( cinfo1->getDestination().toString() == dest1.toString() );
            CPPUNIT_ASSERT( cinfo1->getMessageSelector() == sel1 );
            
            SessionInfo* info2 = connector.createSession( cms::Session::DupsOkAcknowledge );
            std::string sel2 = "mysel2";
            StompTopic dest2( "dummy.topic.2" );
            ConsumerInfo* cinfo2 = connector.createConsumer( &dest2, info2, sel2 );
            CPPUNIT_ASSERT( cinfo2->getSessionInfo() == info2 );
            CPPUNIT_ASSERT( cinfo2->getDestination().toString() == dest2.toString() );
            CPPUNIT_ASSERT( cinfo2->getMessageSelector() == sel2 );
            
            SessionInfo* info3 = connector.createSession( cms::Session::ClientAcknowledge );
            std::string sel3 = "mysel3";
            StompQueue dest3( "dummy.queue.1" );
            ConsumerInfo* cinfo3 = connector.createConsumer( &dest3, info3, sel3 );
            CPPUNIT_ASSERT( cinfo3->getSessionInfo() == info3 );
            CPPUNIT_ASSERT( cinfo3->getDestination().toString() == dest3.toString() );
            CPPUNIT_ASSERT( cinfo3->getMessageSelector() == sel3 );
            
            SessionInfo* info4 = connector.createSession( cms::Session::Transactional );
            std::string sel4 = "";
            StompTopic dest4( "dummy.queue.2" );
            ConsumerInfo* cinfo4 = connector.createConsumer( &dest4, info4, sel4 );
            CPPUNIT_ASSERT( cinfo4->getSessionInfo() == info4 );
            CPPUNIT_ASSERT( cinfo4->getDestination().toString() == dest4.toString() );
            CPPUNIT_ASSERT( cinfo4->getMessageSelector() == sel4 );
            
            connector.destroyResource( cinfo1 );
            connector.destroyResource( cinfo2 );
            connector.destroyResource( cinfo3 );
            connector.destroyResource( cinfo4 );

            connector.destroyResource( info1 );
            connector.destroyResource( info2 );
            connector.destroyResource( info3 );
            connector.destroyResource( info4 );
        }

        void testProducers()
        {
            std::string connectionId = "testConnectionId";
            StompResponseBuilder responseBuilder("testConnectionId");
            transport::DummyTransport transport( &responseBuilder );
            util::SimpleProperties properties;
            StompConnector connector( &transport, properties );
            connector.start();
            
            SessionInfo* info1 = connector.createSession( cms::Session::AutoAcknowledge );
            StompTopic dest1( "dummy.topic.1" );
            ProducerInfo* pinfo1 = connector.createProducer( &dest1, info1 );
            CPPUNIT_ASSERT( pinfo1->getSessionInfo() == info1 );
            CPPUNIT_ASSERT( pinfo1->getDestination().toString() == dest1.toString() );
            
            SessionInfo* info2 = connector.createSession( cms::Session::DupsOkAcknowledge );
            StompTopic dest2( "dummy.topic.2" );
            ProducerInfo* pinfo2 = connector.createProducer( &dest2, info2 );
            CPPUNIT_ASSERT( pinfo2->getSessionInfo() == info2 );
            CPPUNIT_ASSERT( pinfo2->getDestination().toString() == dest2.toString() );
            
            SessionInfo* info3 = connector.createSession( cms::Session::ClientAcknowledge );
            StompQueue dest3( "dummy.queue.1" );
            ProducerInfo* pinfo3 = connector.createProducer( &dest3, info3 );
            CPPUNIT_ASSERT( pinfo3->getSessionInfo() == info3 );
            CPPUNIT_ASSERT( pinfo3->getDestination().toString() == dest3.toString() );
            
            SessionInfo* info4 = connector.createSession( cms::Session::Transactional );
            StompTopic dest4( "dummy.queue.2" );
            ProducerInfo* pinfo4 = connector.createProducer( &dest4, info4 );
            CPPUNIT_ASSERT( pinfo4->getSessionInfo() == info4 );
            CPPUNIT_ASSERT( pinfo4->getDestination().toString() == dest4.toString() );
            
            connector.destroyResource( pinfo1 );
            connector.destroyResource( pinfo2 );
            connector.destroyResource( pinfo3 );
            connector.destroyResource( pinfo4 );

            connector.destroyResource( info1 );
            connector.destroyResource( info2 );
            connector.destroyResource( info3 );
            connector.destroyResource( info4 );
        }
        
        void testCommand()
        {
            std::string connectionId = "testConnectionId";
            StompResponseBuilder responseBuilder("testConnectionId");
            transport::DummyTransport transport( &responseBuilder );
            util::SimpleProperties properties;
            StompConnector connector( &transport, properties );
            connector.start();
            
            StompTopic dest1( "dummy.topic" );
            StompTopic dest2( "dummy.topic2" );
            
            SessionInfo* info1 = connector.createSession( cms::Session::AutoAcknowledge );
            ConsumerInfo* cinfo1 = connector.createConsumer( &dest1, info1, "" );
            
            SessionInfo* info2 = connector.createSession( cms::Session::DupsOkAcknowledge );
            ConsumerInfo* cinfo2 = connector.createConsumer( &dest1, info2, "" );
            
            SessionInfo* info3 = connector.createSession( cms::Session::ClientAcknowledge );
            ConsumerInfo* cinfo3 = connector.createConsumer( &dest2, info3, "" );
            
            SessionInfo* info4 = connector.createSession( cms::Session::Transactional );
            ConsumerInfo* cinfo4 = connector.createConsumer( &dest2, info4, "" );
            
            MyMessageListener listener;
            connector.setConsumerMessageListener( &listener );
            
            StompFrame* frame = new StompFrame();
            frame->setCommand( "MESSAGE" );
            frame->getProperties().setProperty( 
                "destination", dest1.toProviderString() );
            const char* buffer = strdup("hello world");
            frame->setBody( buffer, 12 );

            commands::TextMessageCommand* msg = 
                new commands::TextMessageCommand( frame );
            transport.fireCommand( msg );
            
            CPPUNIT_ASSERT( listener.consumers.size() == 2 );
            for( unsigned int ix=0; ix<listener.consumers.size(); ++ix ){
                CPPUNIT_ASSERT( listener.consumers[ix] == cinfo1 || 
                    listener.consumers[ix] == cinfo2 );
            }
            
            // Clean up the consumers list
            listener.consumers.clear();
            
            frame = new StompFrame();
            frame->setCommand( "MESSAGE" );
            frame->getProperties().setProperty( 
                "destination", dest2.toProviderString() );
            buffer = strdup("hello world");
            frame->setBody( buffer, 12 );

            msg = new commands::TextMessageCommand( frame );
            transport.fireCommand( msg );
            
            CPPUNIT_ASSERT( listener.consumers.size() == 2 );
            for( unsigned int ix=0; ix<listener.consumers.size(); ++ix ){
                CPPUNIT_ASSERT( listener.consumers[ix] == cinfo3 || 
                    listener.consumers[ix] == cinfo4 );
            }
                        
            connector.destroyResource( cinfo1 );
            connector.destroyResource( cinfo2 );
            connector.destroyResource( cinfo3 );
            connector.destroyResource( cinfo4 );

            connector.destroyResource( info1 );
            connector.destroyResource( info2 );
            connector.destroyResource( info3 );
            connector.destroyResource( info4 );
        }
        
        void testSendingCommands()
        {
            std::string connectionId = "testConnectionId";
            StompResponseBuilder responseBuilder("testConnectionId");
            transport::DummyTransport transport( &responseBuilder );
            util::SimpleProperties properties;
            StompConnector* connector = 
                new StompConnector( &transport, properties );
            connector->start();
            
            StompTopic dest1( "dummy.topic.1" );
            
            MyCommandListener cmdListener;
            transport.setOutgoingCommandListener( &cmdListener );
            
            SessionInfo* info1 = connector->createSession( cms::Session::AutoAcknowledge );
            ConsumerInfo* cinfo1 = connector->createConsumer( &dest1, info1, "" );                    
            CPPUNIT_ASSERT( cmdListener.cmd != NULL );
            
            cmdListener.cmd = NULL;
            
            SessionInfo* info2 = connector->createSession( cms::Session::DupsOkAcknowledge );
            ConsumerInfo* cinfo2 = connector->createConsumer( &dest1, info2, "" );
            CPPUNIT_ASSERT( cmdListener.cmd == NULL );
            
            cmdListener.cmd = NULL;
            
            connector->destroyResource( cinfo1 );
            CPPUNIT_ASSERT( cmdListener.cmd == NULL );
            
            cmdListener.cmd = NULL;
            
            connector->destroyResource( cinfo2 );
            CPPUNIT_ASSERT( cmdListener.cmd != NULL );
            
            connector->destroyResource( info1 );
            connector->destroyResource( info2 );
            
            delete connector;
            CPPUNIT_ASSERT( cmdListener.cmd != NULL );
        }

    };

}}}

#endif /*_ACTIVEMQ_CONNECTOR_STOMP_STOMPCONNECTORTEST_H_*/
