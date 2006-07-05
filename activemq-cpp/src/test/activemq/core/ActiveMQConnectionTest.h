#ifndef _ACTIVEMQ_CORE_ACTIVEMQCONNECTIONTEST_H_
#define _ACTIVEMQ_CORE_ACTIVEMQCONNECTIONTEST_H_

#include <cppunit/TestFixture.h>
#include <cppunit/extensions/HelperMacros.h>

#include <activemq/network/Socket.h>
#include <activemq/network/ServerSocket.h>
#include <activemq/concurrent/Concurrent.h>
#include <activemq/concurrent/Mutex.h>
#include <activemq/concurrent/Thread.h>
#include <activemq/core/ActiveMQConnectionFactory.h>
#include <cms/Connection.h>
#include <activemq/transport/DummyTransport.h>
#include <activemq/core/ActiveMQConnection.h>
#include <activemq/core/ActiveMQConnectionData.h>
#include <activemq/connector/ConsumerMessageListener.h>
#include <activemq/connector/ConsumerInfo.h>
#include <activemq/connector/stomp/StompConnector.h>
#include <activemq/util/SimpleProperties.h>
#include <activemq/transport/DummyTransportFactory.h>
#include <activemq/transport/TransportFactoryMap.h>
#include <activemq/transport/TransportFactoryMapRegistrar.h>
#include <activemq/connector/stomp/StompConsumerInfo.h>
#include <activemq/connector/stomp/StompProducerInfo.h>
#include <activemq/connector/stomp/StompTransactionInfo.h>
#include <activemq/connector/stomp/StompSessionInfo.h>
#include <activemq/connector/stomp/StompTopic.h>
#include <activemq/connector/stomp/commands/TextMessageCommand.h>

namespace activemq{
namespace core{

    class ActiveMQConnectionTest : public CppUnit::TestFixture
    {
        CPPUNIT_TEST_SUITE( ActiveMQConnectionTest );
        CPPUNIT_TEST( test );
        CPPUNIT_TEST_SUITE_END();

    public:

    	ActiveMQConnectionTest() {};
    	virtual ~ActiveMQConnectionTest() {}

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
        
        class MyMessageListener : 
            public connector::ConsumerMessageListener
        {
        public:
        
            std::vector<connector::ConsumerInfo*> consumers;
            
        public:
            virtual ~MyMessageListener(){}
            
            virtual void onConsumerMessage( 
                connector::ConsumerInfo* consumer,
                core::ActiveMQMessage* msg )
            {
                consumers.push_back( consumer );
            }
        };

        class MyExceptionListener : public cms::ExceptionListener{
        public:
        
            bool caughtOne;

        public:
        
            MyExceptionListener(){ caughtOne = false; }
            virtual ~MyExceptionListener(){}
            
            virtual void onException(const cms::CMSException& ex){
                caughtOne = true;
            }
        };
        
        class MyActiveMQMessageListener : public ActiveMQMessageListener
        {
        public:
        
            std::vector<ActiveMQMessage*> messages;
            
        public:
            virtual ~MyActiveMQMessageListener(){}
            
            virtual void onActiveMQMessage( ActiveMQMessage* message )
                throw ( exceptions::ActiveMQException )
            {
                messages.push_back( message );
            }
        };

        void test()
        {
            try
            {
                transport::TransportFactoryMapRegistrar registrar(
                    "dummy", new transport::DummyTransportFactory() );

                MyMessageListener listener;
                MyExceptionListener exListener;
                MyCommandListener cmdListener;
                MyActiveMQMessageListener msgListener;
                std::string connectionId = "testConnectionId";
                util::SimpleProperties* properties = 
                    new util::SimpleProperties();
                transport::Transport* transport = NULL;
    
                transport::TransportFactory* factory = 
                    transport::TransportFactoryMap::getInstance().lookup( 
                        "dummy" );
                if( factory == NULL ){
                    CPPUNIT_ASSERT( false );
                }
                
                // Create the transport.
                transport = factory->createTransport( *properties );
                if( transport == NULL ){
                    CPPUNIT_ASSERT( false );
                }
                
                transport::DummyTransport* dTransport = 
                    dynamic_cast< transport::DummyTransport*>( transport );
                    
                CPPUNIT_ASSERT( dTransport != NULL );
                
                dTransport->setCommandListener( &cmdListener );

                connector::stomp::StompConnector* connector = 
                    new connector::stomp::StompConnector( 
                        transport, *properties );

                connector->start();
                
                ActiveMQConnection connection( 
                    new ActiveMQConnectionData(
                        connector, transport, properties) );

                connection.setExceptionListener( &exListener );
                        
                cms::Session* session1 = connection.createSession();
                cms::Session* session2 = connection.createSession();
                cms::Session* session3 = connection.createSession();
                
                CPPUNIT_ASSERT( session1 != NULL );
                CPPUNIT_ASSERT( session2 != NULL );
                CPPUNIT_ASSERT( session3 != NULL );
                
                connector::stomp::StompSessionInfo session;
                connector::stomp::StompConsumerInfo consumer;
                
                session.setSessionId( 1 );
                session.setConnectionId( "TEST:123" );
                session.setAckMode( cms::Session::AutoAcknowledge );
                
                consumer.setConsumerId( 1 );
                consumer.setSessionInfo( &session );
                consumer.setDestination( 
                    connector::stomp::StompTopic( "test" ) );
                    
                connection.addMessageListener( 1, &msgListener );

                connector::stomp::commands::TextMessageCommand* cmd = 
                    new connector::stomp::commands::TextMessageCommand;

                cmd->setCMSDestination( 
                    connector::stomp::StompTopic( "test" ) );
                
                connector::ConsumerMessageListener* consumerListener = 
                    dynamic_cast< connector::ConsumerMessageListener* >( 
                        &connection );

                connection.start();

                CPPUNIT_ASSERT( consumerListener != NULL );
                
                consumerListener->onConsumerMessage( &consumer, cmd );

                CPPUNIT_ASSERT( msgListener.messages.size() == 1 );

                connection.removeMessageListener( 1 );
                
                msgListener.messages.clear();
                consumerListener->onConsumerMessage( &consumer, cmd );
                
                CPPUNIT_ASSERT( msgListener.messages.size() == 0 );

                connection.addMessageListener( 1, &msgListener );

                connection.stop();
                consumerListener->onConsumerMessage( &consumer, cmd );
                connection.start();
                CPPUNIT_ASSERT( msgListener.messages.size() == 0 );

                cmd = new connector::stomp::commands::TextMessageCommand;

                cmd->setCMSDestination( 
                    connector::stomp::StompTopic( "test" ) );

                consumerListener->onConsumerMessage( &consumer, cmd );
                CPPUNIT_ASSERT( msgListener.messages.size() == 1 );

                connection.removeMessageListener( 1 );                
                msgListener.messages.clear();

                connection.close();

                consumerListener->onConsumerMessage( &consumer, cmd );
                CPPUNIT_ASSERT( exListener.caughtOne == true );

                delete cmd;
            }
            catch(...)
            {
                bool exceptionThrown = false;
                
                CPPUNIT_ASSERT( exceptionThrown );
            }
        }

    };

}}

#endif /*_ACTIVEMQ_CORE_ACTIVEMQCONNECTIONTEST_H_*/
