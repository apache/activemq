#ifndef _ACTIVEMQ_CONNECTOR_STOMP_COMMAND_SUBSCRIBECOMMANDTEST_H_
#define _ACTIVEMQ_CONNECTOR_STOMP_COMMAND_SUBSCRIBECOMMANDTEST_H_

#include <cppunit/TestFixture.h>
#include <cppunit/extensions/HelperMacros.h>

#include <activemq/connector/stomp/commands/SubscribeCommand.h>

namespace activemq{
namespace connector{
namespace stomp{
namespace commands{

    class SubscribeCommandTest : public CppUnit::TestFixture
    {
        CPPUNIT_TEST_SUITE( SubscribeCommandTest );
        CPPUNIT_TEST( test );
        CPPUNIT_TEST_SUITE_END();
        
    public:

    	SubscribeCommandTest() {}
    	virtual ~SubscribeCommandTest() {}

        void test(void)
        {
            SubscribeCommand cmd;

            CPPUNIT_ASSERT( cmd.getStompCommandId() == 
                            CommandConstants::SUBSCRIBE );
            
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
            CPPUNIT_ASSERT( cmd.getDestination() == NULL );
            cmd.setDestination( "456987" );
            CPPUNIT_ASSERT( std::string( cmd.getDestination() ) == 
                            "456987" );
            CPPUNIT_ASSERT( cmd.getAckMode()  == 
                            CommandConstants::ACK_AUTO );
            cmd.setAckMode( CommandConstants::ACK_CLIENT );
            CPPUNIT_ASSERT( cmd.getAckMode()  == 
                            CommandConstants::ACK_CLIENT );
            CPPUNIT_ASSERT( cmd.getMessageSelector() == NULL );
            cmd.setMessageSelector( "Selector" );
            CPPUNIT_ASSERT( std::string( cmd.getMessageSelector() ) == 
                            "Selector" );
            CPPUNIT_ASSERT( cmd.getSubscriptionName() == NULL );
            cmd.setSubscriptionName( "subscription" );
            CPPUNIT_ASSERT( std::string( cmd.getSubscriptionName() ) == 
                            "subscription" );
            CPPUNIT_ASSERT( cmd.getNoLocal() == false );
            cmd.setNoLocal( true );
            CPPUNIT_ASSERT( cmd.getNoLocal() == true );
            
            StompFrame* frame = cmd.marshal().clone();
            
            CPPUNIT_ASSERT( frame != NULL );
            
            SubscribeCommand cmd1( frame );
            
            CPPUNIT_ASSERT( cmd.getCommandId() == cmd1.getCommandId() );
            CPPUNIT_ASSERT( cmd.getStompCommandId() == cmd1.getStompCommandId() );
            CPPUNIT_ASSERT( cmd.isResponseRequired() == cmd1.isResponseRequired() );
            CPPUNIT_ASSERT( cmd.getCorrelationId() == cmd1.getCorrelationId() );
            CPPUNIT_ASSERT( std::string(cmd.getTransactionId()) == 
                            cmd1.getTransactionId() );
            CPPUNIT_ASSERT( std::string( cmd.getDestination() ) == 
                            cmd1.getDestination() );
            CPPUNIT_ASSERT( cmd.getAckMode() == cmd1.getAckMode() );
            CPPUNIT_ASSERT( std::string( cmd.getMessageSelector() ) == 
                            cmd1.getMessageSelector() );
            CPPUNIT_ASSERT( std::string( cmd.getSubscriptionName() ) == 
                            cmd1.getSubscriptionName() );
            CPPUNIT_ASSERT( cmd.getNoLocal() == cmd1.getNoLocal() );
            
        }

    };

}}}}

#endif /*_ACTIVEMQ_CONNECTOR_STOMP_COMMAND_SUBSCRIBECOMMANDTEST_H_*/
