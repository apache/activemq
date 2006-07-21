#ifndef _ACTIVEMQ_CONNECTOR_STOMP_COMMANDS_MESSAGECOMMANDTEST_H_
#define _ACTIVEMQ_CONNECTOR_STOMP_COMMANDS_MESSAGECOMMANDTEST_H_

#include <cppunit/TestFixture.h>
#include <cppunit/extensions/HelperMacros.h>

#include <activemq/connector/stomp/commands/MessageCommand.h>
#include <activemq/core/ActiveMQAckHandler.h>
#include <activemq/connector/stomp/StompTopic.h>
#include <cms/Message.h>

namespace activemq{
namespace connector{
namespace stomp{
namespace commands{

    class MessageCommandTest : public CppUnit::TestFixture
    {
        CPPUNIT_TEST_SUITE( MessageCommandTest );
        CPPUNIT_TEST( test );
        CPPUNIT_TEST_SUITE_END();

    protected:
    
        class TestAckHandler : public core::ActiveMQAckHandler
        {
        public:
        
            TestAckHandler(void) { wasAcked = false; }
            virtual ~TestAckHandler(void) {}
            
            virtual void acknowledgeMessage( const core::ActiveMQMessage* message)
                throw ( cms::CMSException ) 
            {
                wasAcked = true;
            }
            
        public:
        
            bool wasAcked;

        };
    
    public:

    	MessageCommandTest() {}
    	virtual ~MessageCommandTest() {}

        void test(void)
        {
            TestAckHandler ackHandler;
            MessageCommand cmd;

            CPPUNIT_ASSERT( cmd.getStompCommandId() == 
                            CommandConstants::SEND );
            
            CPPUNIT_ASSERT( cmd.isResponseRequired() == false );
            cmd.setResponseRequired( true );
            cmd.setCommandId( 123 );
            CPPUNIT_ASSERT( cmd.isResponseRequired() == true );
            CPPUNIT_ASSERT( cmd.getCommandId() == 123 );
            cmd.setCorrelationId( 99 );
            CPPUNIT_ASSERT( cmd.getCorrelationId() == 99 );
            CPPUNIT_ASSERT( cmd.getTransactionId() == NULL );
            cmd.setTransactionId( "ID:123456" );
            CPPUNIT_ASSERT( std::string( cmd.getTransactionId() ) == 
                            "ID:123456" );
            StompTopic topic("testTopic");
            cmd.setCMSDestination( &topic );
            
            StompFrame* frame = cmd.marshal().clone();
            
            CPPUNIT_ASSERT( frame != NULL );
            
            MessageCommand cmd1( frame );
            
            CPPUNIT_ASSERT( cmd.getCommandId() == cmd1.getCommandId() );
            CPPUNIT_ASSERT( cmd.getStompCommandId() == CommandConstants::SEND );
            CPPUNIT_ASSERT( cmd.isResponseRequired() == cmd1.isResponseRequired() );
            CPPUNIT_ASSERT( cmd.getCorrelationId() == cmd1.getCorrelationId() );
            CPPUNIT_ASSERT( std::string(cmd.getTransactionId()) == cmd1.getTransactionId() );
            
            cmd.setAckHandler( &ackHandler );
            cmd.acknowledge();
            CPPUNIT_ASSERT( ackHandler.wasAcked == true );
            
            CPPUNIT_ASSERT( 
                cmd.getProperties().hasProperty( "test" ) == false );
            cmd.getProperties().setProperty( "test", "value" );
            CPPUNIT_ASSERT( 
                cmd.getProperties().hasProperty( "test" ) == true );
            CPPUNIT_ASSERT( 
                std::string( cmd.getProperties().getProperty( "test" ) ) == 
                "value" );
                
            CPPUNIT_ASSERT( cmd.getCMSCorrelationId() == NULL );
            cmd.setCMSCorrelationId( "ID:1234567" );
            CPPUNIT_ASSERT( std::string( cmd.getCMSCorrelationId() ) == 
                            "ID:1234567" );
            CPPUNIT_ASSERT( cmd.getCMSDeliveryMode() == 
                            cms::DeliveryMode::PERSISTANT );
            cmd.setCMSDeliveryMode( cms::DeliveryMode::NON_PERSISTANT );
            CPPUNIT_ASSERT( cmd.getCMSDeliveryMode() == 
                            cms::DeliveryMode::NON_PERSISTANT );
            CPPUNIT_ASSERT( cmd.getCMSDestination()->toString() == 
                            "testTopic" );
            CPPUNIT_ASSERT( cmd.getCMSExpiration() == 0 );
            cmd.setCMSExpiration( 123 );
            CPPUNIT_ASSERT( cmd.getCMSExpiration() == 123 );
            CPPUNIT_ASSERT( cmd.getCMSMessageId() == NULL );
            cmd.setCMSMessageId( "ID:1234567" );
            CPPUNIT_ASSERT( std::string( cmd.getCMSMessageId() ) == 
                            "ID:1234567" );
            CPPUNIT_ASSERT( cmd.getCMSPriority() == 0 );
            cmd.setCMSPriority( 5 );
            CPPUNIT_ASSERT( cmd.getCMSPriority() == 5 );
            CPPUNIT_ASSERT( cmd.getCMSRedelivered() == false );
            cmd.setCMSRedelivered( true );
            CPPUNIT_ASSERT( cmd.getCMSRedelivered() == true );
            CPPUNIT_ASSERT( cmd.getCMSReplyTo() == NULL );
            cmd.setCMSReplyTo( "topic" );
            CPPUNIT_ASSERT( std::string( cmd.getCMSReplyTo() ) == 
                            "topic" );
            CPPUNIT_ASSERT( cmd.getCMSTimeStamp() == 0 );
            cmd.setCMSTimeStamp( 123 );
            CPPUNIT_ASSERT( cmd.getCMSTimeStamp() == 123 );
            CPPUNIT_ASSERT( cmd.getCMSMessageType() == NULL );
            cmd.setCMSMessageType( "test" );
            CPPUNIT_ASSERT( std::string( cmd.getCMSMessageType() ) == 
                            "test" );
            CPPUNIT_ASSERT( cmd.getRedeliveryCount() == 0 );
            cmd.setRedeliveryCount( 123 );
            CPPUNIT_ASSERT( cmd.getRedeliveryCount() == 123 );

            cms::Message* cmd2 = cmd.clone();
            
            CPPUNIT_ASSERT( cmd.getCMSPriority() == cmd2->getCMSPriority() );
            CPPUNIT_ASSERT( cmd.getCMSTimeStamp() == cmd2->getCMSTimeStamp() );
            CPPUNIT_ASSERT( cmd.getCMSExpiration() == cmd2->getCMSExpiration() );
            CPPUNIT_ASSERT( cmd.getCMSDeliveryMode() == cmd2->getCMSDeliveryMode() );
            CPPUNIT_ASSERT( std::string(cmd.getCMSCorrelationId()) == cmd2->getCMSCorrelationId() );
            CPPUNIT_ASSERT( std::string(cmd.getCMSReplyTo()) == cmd2->getCMSReplyTo() );
            CPPUNIT_ASSERT( std::string(cmd.getCMSMessageType()) == cmd2->getCMSMessageType() );
            CPPUNIT_ASSERT( std::string(cmd.getCMSMessageId()) == cmd2->getCMSMessageId() );

            core::ActiveMQMessage* message = 
                dynamic_cast< core::ActiveMQMessage* >( cmd2 );
                
            CPPUNIT_ASSERT( message != NULL );
            CPPUNIT_ASSERT( cmd.getRedeliveryCount() == 
                            message->getRedeliveryCount() );
            
            StompCommand* cmd4 = 
                dynamic_cast< StompCommand* >( cmd2 );

            CPPUNIT_ASSERT( cmd4 != NULL );
            CPPUNIT_ASSERT( cmd.getCommandId() == cmd4->getCommandId() );
            CPPUNIT_ASSERT( cmd.getStompCommandId() == cmd4->getStompCommandId() );
            CPPUNIT_ASSERT( cmd.isResponseRequired() == cmd4->isResponseRequired() );
            CPPUNIT_ASSERT( cmd.getCorrelationId() == cmd4->getCorrelationId() );
            CPPUNIT_ASSERT( std::string(cmd.getTransactionId()) == 
                            cmd4->getTransactionId() );

            delete cmd2;
        }

    };

}}}}

#endif /*_ACTIVEMQ_CONNECTOR_STOMP_COMMANDS_MESSAGECOMMANDTEST_H_*/
