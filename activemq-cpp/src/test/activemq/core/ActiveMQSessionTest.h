#ifndef _ACTIVEMQ_CORE_ACTIVEMQSESSIONTEST_H_
#define _ACTIVEMQ_CORE_ACTIVEMQSESSIONTEST_H_

#include <cppunit/TestFixture.h>
#include <cppunit/extensions/HelperMacros.h>

#include <cms/Connection.h>
#include <cms/MessageListener.h>

#include <activemq/network/Socket.h>
#include <activemq/network/ServerSocket.h>
#include <activemq/concurrent/Concurrent.h>
#include <activemq/concurrent/Mutex.h>
#include <activemq/concurrent/Thread.h>
#include <activemq/core/ActiveMQConnectionFactory.h>
#include <activemq/core/ActiveMQConnection.h>
#include <activemq/core/ActiveMQConnectionData.h>
#include <activemq/core/ActiveMQSession.h>
#include <activemq/core/ActiveMQConsumer.h>
#include <activemq/core/ActiveMQProducer.h>
#include <activemq/util/SimpleProperties.h>
#include <activemq/transport/DummyTransport.h>
#include <activemq/transport/DummyTransportFactory.h>
#include <activemq/transport/TransportFactoryMap.h>
#include <activemq/transport/TransportFactoryMapRegistrar.h>
#include <activemq/connector/ConsumerMessageListener.h>
#include <activemq/connector/ConsumerInfo.h>
#include <activemq/connector/stomp/StompConnector.h>
#include <activemq/connector/stomp/StompConsumerInfo.h>
#include <activemq/connector/stomp/StompProducerInfo.h>
#include <activemq/connector/stomp/StompTransactionInfo.h>
#include <activemq/connector/stomp/StompSessionInfo.h>
#include <activemq/connector/stomp/StompTopic.h>
#include <activemq/connector/stomp/commands/TextMessageCommand.h>

namespace activemq{
namespace core{

    class ActiveMQSessionTest : public CppUnit::TestFixture
    {
        CPPUNIT_TEST_SUITE( ActiveMQSessionTest );
        CPPUNIT_TEST( testAutoAcking );
        CPPUNIT_TEST( testClientAck );
        CPPUNIT_TEST( testTransactional );
        CPPUNIT_TEST_SUITE_END();
        
    private:
    
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
        
        class MyCMSMessageListener : public cms::MessageListener
        {
        public:
        
            std::vector<cms::Message*> messages;
            concurrent::Mutex mutex;
            bool ack;
            
        public:

            MyCMSMessageListener( bool ack = false ){
                this->ack = ack;
            }

            virtual ~MyCMSMessageListener(){
                clear();
            }

            virtual void setAck( bool ack ){
                this->ack = ack;
            }

            virtual void clear() {
                std::vector<cms::Message*>::iterator itr = 
                    messages.begin();
                    
                for( ; itr != messages.end(); ++itr )
                {
                    delete *itr;
                }

                messages.clear();
            }
            
            virtual void onMessage( const cms::Message& message )
            {
                synchronized( &mutex )
                {
                    if( ack ){
                        message.acknowledge();
                    }

                    messages.push_back( message.clone() );

                    mutex.notifyAll();
                }
            }
        };

        ActiveMQConnection* connection;
        transport::DummyTransport* dTransport;
        MyExceptionListener exListener;
        MyCommandListener cmdListener;

    public:    // CPPUNIT Method Overrides.
    
        void setUp()
        {
            try
            {
                transport::TransportFactoryMapRegistrar registrar(
                    "dummy", new transport::DummyTransportFactory() );
    
                ActiveMQConnectionFactory factory("dummy://127.0.0.1:12345");
                
                connection = dynamic_cast< ActiveMQConnection*>( 
                    factory.createConnection() );

                // Get the Transport and make sure we got a dummy Transport
                // then add our command listener, so we can verify that when
                // we send a message it hits the wire.
                dTransport = dynamic_cast< transport::DummyTransport*>( 
                    connection->getConnectionData()->getTransport() );
                CPPUNIT_ASSERT( dTransport != NULL );                
                dTransport->setOutgoingCommandListener( &cmdListener );

                connection->setExceptionListener( &exListener );
                connection->start();
            }
            catch(...)
            {
                bool exceptionThrown = false;
                
                CPPUNIT_ASSERT( exceptionThrown );
            }
        }
        
        void tearDown()
        {
            delete connection;
        }
        
        void injectTextMessage( const std::string message,
                                const cms::Destination& destination )
        {
            connector::stomp::StompFrame* frame = 
                new connector::stomp::StompFrame();
            frame->setCommand( "MESSAGE" );
            frame->getProperties().setProperty( 
                "destination", destination.toProviderString() );
            const char* buffer = strdup( message.c_str() );
            frame->setBody( buffer, 12 );

            connector::stomp::commands::TextMessageCommand* msg = 
                new connector::stomp::commands::TextMessageCommand( frame );

            // Init Message
            msg->setText( message.c_str() );
            msg->setCMSDestination( destination );
            msg->setCMSMessageId( "Id: 123456" );

            // Send the Message
            CPPUNIT_ASSERT( dTransport != NULL );
            
            dTransport->fireCommand( msg );
        }
        
    public:

    	ActiveMQSessionTest(void) {}
    	virtual ~ActiveMQSessionTest(void) {}

        void testAutoAcking()
        {
            MyCMSMessageListener msgListener1;
            MyCMSMessageListener msgListener2;
            
            CPPUNIT_ASSERT( connection != NULL );
            
            // Create an Auto Ack Session
            cms::Session* session = connection->createSession();

            // Create a Topic
            cms::Topic* topic1 = session->createTopic( "TestTopic1");
            cms::Topic* topic2 = session->createTopic( "TestTopic2");
            
            CPPUNIT_ASSERT( topic1 != NULL );                
            CPPUNIT_ASSERT( topic2 != NULL );                

            // Create a consumer
            cms::MessageConsumer* consumer1 = 
                session->createConsumer( *topic1 );
            cms::MessageConsumer* consumer2 = 
                session->createConsumer( *topic2 );

            CPPUNIT_ASSERT( consumer1 != NULL );                
            CPPUNIT_ASSERT( consumer2 != NULL );
            
            CPPUNIT_ASSERT( consumer1->getMessageSelector() == "" );            
            CPPUNIT_ASSERT( consumer2->getMessageSelector() == "" );            

            CPPUNIT_ASSERT( consumer1->receiveNoWait() == NULL );
            CPPUNIT_ASSERT( consumer1->receive( 5 ) == NULL );
            CPPUNIT_ASSERT( consumer2->receiveNoWait() == NULL );
            CPPUNIT_ASSERT( consumer2->receive( 5 ) == NULL );

            consumer1->setMessageListener( &msgListener1 );
            consumer2->setMessageListener( &msgListener2 );
            
            injectTextMessage( "This is a Test 1" , *topic1 );

            synchronized( &msgListener1.mutex )
            {
                if( msgListener1.messages.size() == 0 )
                {
                    msgListener1.mutex.wait( 3000 );
                }
            }

            CPPUNIT_ASSERT( msgListener1.messages.size() == 1 );

            injectTextMessage( "This is a Test 2" , *topic2 );

            synchronized( &msgListener2.mutex )
            {
                if( msgListener2.messages.size() == 0 )
                {
                    msgListener2.mutex.wait( 3000 );
                }
            }

            CPPUNIT_ASSERT( msgListener2.messages.size() == 1 );
            
            cms::TextMessage* msg1 = 
                dynamic_cast< cms::TextMessage* >( 
                    msgListener1.messages[0] );
            cms::TextMessage* msg2 = 
                dynamic_cast< cms::TextMessage* >( 
                    msgListener2.messages[0] );

            CPPUNIT_ASSERT( msg1 != NULL );                
            CPPUNIT_ASSERT( msg2 != NULL );                
            
            std::string text1 = msg1->getText();
            std::string text2 = msg2->getText();
            
            CPPUNIT_ASSERT( text1 == "This is a Test 1" );
            CPPUNIT_ASSERT( text2 == "This is a Test 2" );

            delete consumer1;
            delete consumer2;

            delete session;
        }

        void testClientAck()
        {
            MyCMSMessageListener msgListener1( true );
            MyCMSMessageListener msgListener2( true );
            
            CPPUNIT_ASSERT( connection != NULL );
            
            // Create an Auto Ack Session
            cms::Session* session = connection->createSession( 
                cms::Session::ClientAcknowledge );

            // Create a Topic
            cms::Topic* topic1 = session->createTopic( "TestTopic1");
            cms::Topic* topic2 = session->createTopic( "TestTopic2");
            
            CPPUNIT_ASSERT( topic1 != NULL );                
            CPPUNIT_ASSERT( topic2 != NULL );                

            // Create a consumer
            cms::MessageConsumer* consumer1 = 
                session->createConsumer( *topic1 );
            cms::MessageConsumer* consumer2 = 
                session->createConsumer( *topic2 );

            CPPUNIT_ASSERT( consumer1 != NULL );                
            CPPUNIT_ASSERT( consumer2 != NULL );
            
            CPPUNIT_ASSERT( consumer1->getMessageSelector() == "" );            
            CPPUNIT_ASSERT( consumer2->getMessageSelector() == "" );            

            CPPUNIT_ASSERT( consumer1->receiveNoWait() == NULL );
            CPPUNIT_ASSERT( consumer1->receive( 5 ) == NULL );
            CPPUNIT_ASSERT( consumer2->receiveNoWait() == NULL );
            CPPUNIT_ASSERT( consumer2->receive( 5 ) == NULL );

            consumer1->setMessageListener( &msgListener1 );
            consumer2->setMessageListener( &msgListener2 );
            
            injectTextMessage( "This is a Test 1" , *topic1 );

            synchronized( &msgListener1.mutex )
            {
                if( msgListener1.messages.size() == 0 )
                {
                    msgListener1.mutex.wait( 3000 );
                }
            }

            CPPUNIT_ASSERT( msgListener1.messages.size() == 1 );

            msgListener1.messages[0]->acknowledge();

            injectTextMessage( "This is a Test 2" , *topic2 );

            synchronized( &msgListener2.mutex )
            {
                if( msgListener2.messages.size() == 0 )
                {
                    msgListener2.mutex.wait( 3000 );
                }
            }

            CPPUNIT_ASSERT( msgListener2.messages.size() == 1 );
            
            msgListener2.messages[0]->acknowledge();

            cms::TextMessage* msg1 = 
                dynamic_cast< cms::TextMessage* >( 
                    msgListener1.messages[0] );
            cms::TextMessage* msg2 = 
                dynamic_cast< cms::TextMessage* >( 
                    msgListener2.messages[0] );

            CPPUNIT_ASSERT( msg1 != NULL );                
            CPPUNIT_ASSERT( msg2 != NULL );                
            
            std::string text1 = msg1->getText();
            std::string text2 = msg2->getText();
            
            CPPUNIT_ASSERT( text1 == "This is a Test 1" );
            CPPUNIT_ASSERT( text2 == "This is a Test 2" );

            delete consumer1;
            delete consumer2;

            delete session;
        }

        void testTransactional()
        {
            MyCMSMessageListener msgListener1;
            MyCMSMessageListener msgListener2;
            
            CPPUNIT_ASSERT( connection != NULL );
            
            // Create an Auto Ack Session
            cms::Session* session = connection->createSession( 
                cms::Session::Transactional );

            // Create a Topic
            cms::Topic* topic1 = session->createTopic( "TestTopic1");
            cms::Topic* topic2 = session->createTopic( "TestTopic2");
            
            CPPUNIT_ASSERT( topic1 != NULL );                
            CPPUNIT_ASSERT( topic2 != NULL );                

            // Create a consumer
            cms::MessageConsumer* consumer1 = 
                session->createConsumer( *topic1 );
            cms::MessageConsumer* consumer2 = 
                session->createConsumer( *topic2 );

            CPPUNIT_ASSERT( consumer1 != NULL );                
            CPPUNIT_ASSERT( consumer2 != NULL );
            
            CPPUNIT_ASSERT( consumer1->getMessageSelector() == "" );            
            CPPUNIT_ASSERT( consumer2->getMessageSelector() == "" );            

            CPPUNIT_ASSERT( consumer1->receiveNoWait() == NULL );
            CPPUNIT_ASSERT( consumer1->receive( 5 ) == NULL );
            CPPUNIT_ASSERT( consumer2->receiveNoWait() == NULL );
            CPPUNIT_ASSERT( consumer2->receive( 5 ) == NULL );

            consumer1->setMessageListener( &msgListener1 );
            consumer2->setMessageListener( &msgListener2 );
            
            injectTextMessage( "This is a Test 1" , *topic1 );

            synchronized( &msgListener1.mutex )
            {
                if( msgListener1.messages.size() == 0 )
                {
                    msgListener1.mutex.wait( 3000 );
                }
            }

            CPPUNIT_ASSERT( msgListener1.messages.size() == 1 );

            session->commit();

            injectTextMessage( "This is a Test 2" , *topic2 );

            synchronized( &msgListener2.mutex )
            {
                if( msgListener2.messages.size() == 0 )
                {
                    msgListener2.mutex.wait( 3000 );
                }
            }

            CPPUNIT_ASSERT( msgListener2.messages.size() == 1 );
            
            session->commit();

            cms::TextMessage* msg1 = 
                dynamic_cast< cms::TextMessage* >( 
                    msgListener1.messages[0] );
            cms::TextMessage* msg2 = 
                dynamic_cast< cms::TextMessage* >( 
                    msgListener2.messages[0] );

            CPPUNIT_ASSERT( msg1 != NULL );                
            CPPUNIT_ASSERT( msg2 != NULL );                
            
            std::string text1 = msg1->getText();
            std::string text2 = msg2->getText();
            
            CPPUNIT_ASSERT( text1 == "This is a Test 1" );
            CPPUNIT_ASSERT( text2 == "This is a Test 2" );

            msgListener1.clear();
            msgListener2.clear();

            const unsigned int msgCount = 50;

            for( unsigned int i = 0; i < msgCount; ++i )
            {
                std::ostringstream stream;

                stream << "This is test message #" << i << std::ends;

                injectTextMessage( stream.str() , *topic1 );
            }
        
            for( unsigned int i = 0; i < msgCount; ++i )
            {
                std::ostringstream stream;

                stream << "This is test message #" << i << std::ends;

                injectTextMessage( stream.str() , *topic2 );
            }

            synchronized( &msgListener1.mutex )
            {
                const unsigned int interval = msgCount + 10;
                unsigned int count = 0;

                while( msgListener1.messages.size() != msgCount && 
                       count < interval )
                {
                    msgListener1.mutex.wait( 3000 );

                    ++count;
                }
            }

            CPPUNIT_ASSERT( msgListener1.messages.size() == msgCount );

            synchronized( &msgListener2.mutex )
            {
                const int interval = msgCount + 10;
                int count = 0;

                while( msgListener2.messages.size() != msgCount && 
                       count < interval )
                {
                    msgListener2.mutex.wait( 3000 );

                    ++count;
                }
            }

            CPPUNIT_ASSERT( msgListener2.messages.size() == msgCount );

            msgListener1.clear();
            msgListener2.clear();

            session->rollback();

            synchronized( &msgListener1.mutex )
            {
                const int interval = msgCount + 10;
                int count = 0;

                while( msgListener1.messages.size() != msgCount && 
                       count < interval )
                {
                    msgListener1.mutex.wait( 3000 );

                    ++count;
                }
            }

            CPPUNIT_ASSERT( msgListener1.messages.size() == msgCount );

            synchronized( &msgListener2.mutex )
            {
                const int interval = msgCount + 10;
                int count = 0;

                while( msgListener2.messages.size() != msgCount && 
                       count < interval )
                {
                    msgListener2.mutex.wait( 3000 );

                    ++count;
                }
            }

            CPPUNIT_ASSERT( msgListener2.messages.size() == msgCount );

            delete consumer1;
            delete consumer2;

            delete session;
        }

    };

}}

#endif /*_ACTIVEMQ_CORE_ACTIVEMQSESSIONTEST_H_*/
