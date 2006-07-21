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
#include <activemq/connector/stomp/commands/SubscribeCommand.h>
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
        CPPUNIT_TEST( testSubscribeOptions );
        CPPUNIT_TEST_SUITE_END();

    public:
    
        typedef std::pair< std::string, std::string > MyProperty;
    
        class MyCommandListener : public transport::CommandListener{
        public:
        
            transport::Command* cmd;
            commands::SubscribeCommand* subscribe;
            
            // Properties that should be in an command
            std::vector< MyProperty > expected;
            
        public:
        
            MyCommandListener(){
                cmd = NULL;
                subscribe = NULL;
            }
            virtual ~MyCommandListener(){}
            
            virtual void onCommand( transport::Command* command ){
                cmd = command;

                subscribe = dynamic_cast< commands::SubscribeCommand* >( command );
                if( subscribe )
                {
                    const util::Properties& properties = 
                        subscribe->getProperties();
                    
                    for( size_t ix = 0; ix < expected.size(); ++ix )
                    {
                        std::string value = 
                            properties.getProperty( expected[ix].first, "" );

                        CPPUNIT_ASSERT( value == expected[ix].second );
                    }
                }
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

                delete msg;
            }
        };

        virtual ~StompSessionManagerTest() {}

        void testSessions()
        {
            std::string connectionId = "testConnectionId";
            StompResponseBuilder responseBuilder("testSessionId");
            transport::DummyTransport transport( &responseBuilder );
            StompSessionManager manager( connectionId, &transport );
            
            SessionInfo* info1 = manager.createSession( cms::Session::AUTO_ACKNOWLEDGE );
            CPPUNIT_ASSERT( info1->getAckMode() == cms::Session::AUTO_ACKNOWLEDGE );
            CPPUNIT_ASSERT( info1->getConnectionId() == connectionId );
            
            SessionInfo* info2 = manager.createSession( cms::Session::DUPS_OK_ACKNOWLEDGE );
            CPPUNIT_ASSERT( info2->getAckMode() == cms::Session::DUPS_OK_ACKNOWLEDGE );
            CPPUNIT_ASSERT( info2->getConnectionId() == connectionId );
            
            SessionInfo* info3 = manager.createSession( cms::Session::CLIENT_ACKNOWLEDGE );
            CPPUNIT_ASSERT( info3->getAckMode() == cms::Session::CLIENT_ACKNOWLEDGE );
            CPPUNIT_ASSERT( info3->getConnectionId() == connectionId );
            
            SessionInfo* info4 = manager.createSession( cms::Session::SESSION_TRANSACTED );
            CPPUNIT_ASSERT( info4->getAckMode() == cms::Session::SESSION_TRANSACTED );
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
            
            SessionInfo* info1 = manager.createSession( cms::Session::AUTO_ACKNOWLEDGE );
            std::string sel1 = "";
            StompTopic dest1( "dummy.topic.1" );
            ConsumerInfo* cinfo1 = manager.createConsumer( &dest1, info1, sel1 );
            CPPUNIT_ASSERT( cinfo1->getSessionInfo() == info1 );
            CPPUNIT_ASSERT( cinfo1->getDestination().toString() == dest1.toString() );
            CPPUNIT_ASSERT( cinfo1->getMessageSelector() == sel1 );
            
            SessionInfo* info2 = manager.createSession( cms::Session::DUPS_OK_ACKNOWLEDGE );
            std::string sel2 = "mysel2";
            StompTopic dest2( "dummy.topic.2" );
            ConsumerInfo* cinfo2 = manager.createConsumer( &dest2, info2, sel2 );
            CPPUNIT_ASSERT( cinfo2->getSessionInfo() == info2 );
            CPPUNIT_ASSERT( cinfo2->getDestination().toString() == dest2.toString() );
            CPPUNIT_ASSERT( cinfo2->getMessageSelector() == sel2 );
            
            SessionInfo* info3 = manager.createSession( cms::Session::CLIENT_ACKNOWLEDGE );
            std::string sel3 = "mysel3";
            StompQueue dest3( "dummy.queue.1" );
            ConsumerInfo* cinfo3 = manager.createConsumer( &dest3, info3, sel3 );
            CPPUNIT_ASSERT( cinfo3->getSessionInfo() == info3 );
            CPPUNIT_ASSERT( cinfo3->getDestination().toString() == dest3.toString() );
            CPPUNIT_ASSERT( cinfo3->getMessageSelector() == sel3 );
            
            SessionInfo* info4 = manager.createSession( cms::Session::SESSION_TRANSACTED );
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
            
            SessionInfo* info1 = manager.createSession( cms::Session::AUTO_ACKNOWLEDGE );
            ConsumerInfo* cinfo1 = manager.createConsumer( &dest1, info1, "" );
            
            SessionInfo* info2 = manager.createSession( cms::Session::DUPS_OK_ACKNOWLEDGE );
            ConsumerInfo* cinfo2 = manager.createConsumer( &dest1, info2, "" );
            
            SessionInfo* info3 = manager.createSession( cms::Session::CLIENT_ACKNOWLEDGE );
            ConsumerInfo* cinfo3 = manager.createConsumer( &dest2, info3, "" );
            
            SessionInfo* info4 = manager.createSession( cms::Session::SESSION_TRANSACTED );
            ConsumerInfo* cinfo4 = manager.createConsumer( &dest2, info4, "" );
            
            MyMessageListener listener;
            manager.setConsumerMessageListener( &listener );
            
            commands::TextMessageCommand* msg = new commands::TextMessageCommand();
            msg->setCMSDestination( &dest1 );
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
            msg->setCMSDestination( &dest2 );
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
            
            SessionInfo* info1 = manager.createSession( cms::Session::AUTO_ACKNOWLEDGE );
            ConsumerInfo* cinfo1 = manager.createConsumer( &dest1, info1, "" );                    
            CPPUNIT_ASSERT( cmdListener.cmd != NULL );
            
            cmdListener.cmd = NULL;
            
            SessionInfo* info2 = manager.createSession( cms::Session::DUPS_OK_ACKNOWLEDGE );
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
      
        void testSubscribeOptions(){
            
            std::string connectionId = "testConnectionId";
            StompResponseBuilder responseBuilder("testSessionId");
            transport::DummyTransport transport( &responseBuilder );
            StompSessionManager manager( connectionId, &transport );
            
            MyProperty retroactive = 
                std::make_pair( "activemq.retroactive", "true" );
            MyProperty prefetchSize = 
                std::make_pair( "activemq.prefetchSize", "1000" );
            MyProperty maxPendingMsgLimit = 
                std::make_pair( "activemq.maximumPendingMessageLimit", "0" );
            MyProperty noLocal = 
                std::make_pair( "activemq.noLocal", "true" );
            MyProperty dispatchAsync = 
                std::make_pair( "activemq.dispatchAsync", "true" );
            MyProperty selector = 
                std::make_pair( "selector", "test" );
            MyProperty exclusive = 
                std::make_pair( "activemq.exclusive", "true" );
            MyProperty priority = 
                std::make_pair( "activemq.priority", "1" );
            
            SessionInfo* session = NULL;
            ConsumerInfo* consumer = NULL;

            MyCommandListener cmdListener;
            transport.setOutgoingCommandListener( &cmdListener );

            session = manager.createSession( cms::Session::AUTO_ACKNOWLEDGE );

            cmdListener.expected.clear();
            StompTopic dest1( "dummy.topic.1" );            
            consumer = manager.createConsumer( &dest1, session, "" );                    
            CPPUNIT_ASSERT( consumer != NULL );
            CPPUNIT_ASSERT( cmdListener.subscribe != NULL );            

            manager.removeConsumer( consumer );
            CPPUNIT_ASSERT( cmdListener.cmd != NULL );
            delete consumer;
            cmdListener.cmd = NULL;
            cmdListener.subscribe = NULL;

            cmdListener.expected.clear();
            cmdListener.expected.push_back( retroactive );            
            StompTopic dest2( "dummy.topic.1?consumer.retroactive=true" );
            consumer = manager.createConsumer( &dest2, session, "" );                    
            CPPUNIT_ASSERT( consumer != NULL );
            CPPUNIT_ASSERT( cmdListener.subscribe != NULL );            

            manager.removeConsumer( consumer );
            CPPUNIT_ASSERT( cmdListener.cmd != NULL );
            delete consumer;
            cmdListener.cmd = NULL;
            cmdListener.subscribe = NULL;

            cmdListener.expected.clear();
            cmdListener.expected.push_back( retroactive );         
            cmdListener.expected.push_back( prefetchSize );         
            cmdListener.expected.push_back( maxPendingMsgLimit );         
            cmdListener.expected.push_back( noLocal );         
            cmdListener.expected.push_back( dispatchAsync );         
            cmdListener.expected.push_back( selector );         
            cmdListener.expected.push_back( exclusive );         
            cmdListener.expected.push_back( priority );         
            StompTopic dest3( 
                std::string( "dummy.topic.1?" ) + 
                "consumer.retroactive=" + retroactive.second + "&" +
                "consumer.prefetchSize=" + prefetchSize.second + "&" +
                "consumer.maximumPendingMessageLimit=" + maxPendingMsgLimit.second + "&" +
                "consumer.noLocal=" + noLocal.second + "&" +
                "consumer.dispatchAsync=" + dispatchAsync.second + "&" +
                "consumer.selector=" + selector.second + "&" +
                "consumer.exclusive=" + exclusive.second + "&" +
                "consumer.priority=" + priority.second );
            consumer = manager.createConsumer( &dest3, session, "" );                    
            CPPUNIT_ASSERT( consumer != NULL );
            CPPUNIT_ASSERT( cmdListener.subscribe != NULL );            

            manager.removeConsumer( consumer );
            CPPUNIT_ASSERT( cmdListener.cmd != NULL );
            delete consumer;
            cmdListener.cmd = NULL;
            cmdListener.subscribe = NULL;

            // Done
            delete session;

        }

    };

}}}

#endif /*_ACTIVEMQ_CONNECTOR_STOMP_STOMPSESSIONMANAGERTEST_H_*/
