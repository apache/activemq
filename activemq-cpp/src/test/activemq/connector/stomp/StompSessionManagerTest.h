#ifndef _ACTIVEMQ_CONNECTOR_STOMP_STOMPSESSIONMANAGERTEST_H_
#define _ACTIVEMQ_CONNECTOR_STOMP_STOMPSESSIONMANAGERTEST_H_

#include <cppunit/TestFixture.h>
#include <cppunit/extensions/HelperMacros.h>

#include <activemq/connector/stomp/StompSessionManager.h>
#include <activemq/connector/stomp/StompResponseBuilder.h>
#include <activemq/connector/stomp/StompTopic.h>
#include <activemq/connector/stomp/StompQueue.h>
#include <activemq/transport/DummyTransport.h>
#include <activemq/connector/stomp/commands/TextMessageCommand.h>
#include <activemq/transport/CommandListener.h>
#include <cms/Session.h>
#include <vector>

namespace activemq{
namespace connector{
namespace stomp{

    class StompSessionManagerTest : public CppUnit::TestFixture
    {

        CPPUNIT_TEST_SUITE( StompSessionManagerTest );
        CPPUNIT_TEST( testSessions );
        CPPUNIT_TEST( testConsumers );
        CPPUNIT_TEST( testCommand );
        CPPUNIT_TEST( testSendingCommands );
        CPPUNIT_TEST_SUITE_END();

    public:
    
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

        virtual ~StompSessionManagerTest() {}

        void testSessions()
        {
            std::string connectionId = "testConnectionId";
            StompResponseBuilder responseBuilder("testSessionId");
            transport::DummyTransport transport( &responseBuilder );
            StompSessionManager manager( connectionId, &transport );
            
            SessionInfo* info1 = manager.createSession( cms::Session::AutoAcknowledge );
            CPPUNIT_ASSERT( info1->getAckMode() == cms::Session::AutoAcknowledge );
            CPPUNIT_ASSERT( info1->getConnectionId() == connectionId );
            
            SessionInfo* info2 = manager.createSession( cms::Session::DupsOkAcknowledge );
            CPPUNIT_ASSERT( info2->getAckMode() == cms::Session::DupsOkAcknowledge );
            CPPUNIT_ASSERT( info2->getConnectionId() == connectionId );
            
            SessionInfo* info3 = manager.createSession( cms::Session::ClientAcknowledge );
            CPPUNIT_ASSERT( info3->getAckMode() == cms::Session::ClientAcknowledge );
            CPPUNIT_ASSERT( info3->getConnectionId() == connectionId );
            
            SessionInfo* info4 = manager.createSession( cms::Session::Transactional );
            CPPUNIT_ASSERT( info4->getAckMode() == cms::Session::Transactional );
            CPPUNIT_ASSERT( info4->getConnectionId() == connectionId );
            
            delete info1;
            delete info2;
            delete info3;
            delete info4;
        }
        
        void testConsumers()
        {
            std::string connectionId = "testConnectionId";
            StompResponseBuilder responseBuilder("testSessionId");
            transport::DummyTransport transport( &responseBuilder );
            StompSessionManager manager( connectionId, &transport );
            
            SessionInfo* info1 = manager.createSession( cms::Session::AutoAcknowledge );
            std::string sel1 = "";
            StompTopic dest1( "dummy.topic.1" );
            ConsumerInfo* cinfo1 = manager.createConsumer( &dest1, info1, sel1 );
            CPPUNIT_ASSERT( cinfo1->getSessionInfo() == info1 );
            CPPUNIT_ASSERT( cinfo1->getDestination().toString() == dest1.toString() );
            CPPUNIT_ASSERT( cinfo1->getMessageSelector() == sel1 );
            
            SessionInfo* info2 = manager.createSession( cms::Session::DupsOkAcknowledge );
            std::string sel2 = "mysel2";
            StompTopic dest2( "dummy.topic.2" );
            ConsumerInfo* cinfo2 = manager.createConsumer( &dest2, info2, sel2 );
            CPPUNIT_ASSERT( cinfo2->getSessionInfo() == info2 );
            CPPUNIT_ASSERT( cinfo2->getDestination().toString() == dest2.toString() );
            CPPUNIT_ASSERT( cinfo2->getMessageSelector() == sel2 );
            
            SessionInfo* info3 = manager.createSession( cms::Session::ClientAcknowledge );
            std::string sel3 = "mysel3";
            StompQueue dest3( "dummy.queue.1" );
            ConsumerInfo* cinfo3 = manager.createConsumer( &dest3, info3, sel3 );
            CPPUNIT_ASSERT( cinfo3->getSessionInfo() == info3 );
            CPPUNIT_ASSERT( cinfo3->getDestination().toString() == dest3.toString() );
            CPPUNIT_ASSERT( cinfo3->getMessageSelector() == sel3 );
            
            SessionInfo* info4 = manager.createSession( cms::Session::Transactional );
            std::string sel4 = "";
            StompTopic dest4( "dummy.queue.2" );
            ConsumerInfo* cinfo4 = manager.createConsumer( &dest4, info4, sel4 );
            CPPUNIT_ASSERT( cinfo4->getSessionInfo() == info4 );
            CPPUNIT_ASSERT( cinfo4->getDestination().toString() == dest4.toString() );
            CPPUNIT_ASSERT( cinfo4->getMessageSelector() == sel4 );
            
            delete info1;
            delete info2;
            delete info3;
            delete info4;
            
            delete cinfo1;
            delete cinfo2;
            delete cinfo3;
            delete cinfo4;
        }
        
        void testCommand()
        {
            std::string connectionId = "testConnectionId";
            StompResponseBuilder responseBuilder("testSessionId");
            transport::DummyTransport transport( &responseBuilder );
            StompSessionManager manager( connectionId, &transport );
            
            StompTopic dest1( "dummy.topic" );
            StompTopic dest2( "dummy.topic2" );
            
            SessionInfo* info1 = manager.createSession( cms::Session::AutoAcknowledge );
            ConsumerInfo* cinfo1 = manager.createConsumer( &dest1, info1, "" );
            
            SessionInfo* info2 = manager.createSession( cms::Session::DupsOkAcknowledge );
            ConsumerInfo* cinfo2 = manager.createConsumer( &dest1, info2, "" );
            
            SessionInfo* info3 = manager.createSession( cms::Session::ClientAcknowledge );
            ConsumerInfo* cinfo3 = manager.createConsumer( &dest2, info3, "" );
            
            SessionInfo* info4 = manager.createSession( cms::Session::Transactional );
            ConsumerInfo* cinfo4 = manager.createConsumer( &dest2, info4, "" );
            
            MyMessageListener listener;
            manager.setConsumerMessageListener( &listener );
            
            commands::TextMessageCommand* msg = new commands::TextMessageCommand();
            msg->setCMSDestination( dest1 );
            msg->setText( "hello world" );                        
            manager.onStompCommand( msg );
            
            CPPUNIT_ASSERT( listener.consumers.size() == 2 );
            for( unsigned int ix=0; ix<listener.consumers.size(); ++ix ){
                CPPUNIT_ASSERT( listener.consumers[ix] == cinfo1 || 
                    listener.consumers[ix] == cinfo2 );
            }
            
            // Clean up the consumers list
            listener.consumers.clear();
            
            msg = new commands::TextMessageCommand();
            msg->setCMSDestination( dest2 );
            msg->setText( "hello world" );
            manager.onStompCommand( msg );
            
            CPPUNIT_ASSERT( listener.consumers.size() == 2 );
            for( unsigned int ix=0; ix<listener.consumers.size(); ++ix ){
                CPPUNIT_ASSERT( listener.consumers[ix] == cinfo3 || 
                    listener.consumers[ix] == cinfo4 );
            }
                        
            delete info1;
            delete info2;
            delete info3;
            delete info4;
            
            delete cinfo1;
            delete cinfo2;
            delete cinfo3;
            delete cinfo4;
        }
        
        void testSendingCommands(){
            
            
            
            std::string connectionId = "testConnectionId";
            StompResponseBuilder responseBuilder("testSessionId");
            transport::DummyTransport transport( &responseBuilder );
            StompSessionManager manager( connectionId, &transport );
            
            StompTopic dest1( "dummy.topic.1" );
            
            MyCommandListener cmdListener;
            transport.setOutgoingCommandListener( &cmdListener );
            
            SessionInfo* info1 = manager.createSession( cms::Session::AutoAcknowledge );
            ConsumerInfo* cinfo1 = manager.createConsumer( &dest1, info1, "" );                    
            CPPUNIT_ASSERT( cmdListener.cmd != NULL );
            
            cmdListener.cmd = NULL;
            
            SessionInfo* info2 = manager.createSession( cms::Session::DupsOkAcknowledge );
            ConsumerInfo* cinfo2 = manager.createConsumer( &dest1, info2, "" );
            CPPUNIT_ASSERT( cmdListener.cmd == NULL );
            
            cmdListener.cmd = NULL;
            
            manager.removeConsumer( cinfo1 );
            CPPUNIT_ASSERT( cmdListener.cmd == NULL );
            
            cmdListener.cmd = NULL;
            
            manager.removeConsumer( cinfo2 );
            CPPUNIT_ASSERT( cmdListener.cmd != NULL );
            
            delete info1;
            delete info2;
            
            delete cinfo1;
            delete cinfo2;                      
        }
      
    };

}}}

#endif /*_ACTIVEMQ_CONNECTOR_STOMP_STOMPSESSIONMANAGERTEST_H_*/
