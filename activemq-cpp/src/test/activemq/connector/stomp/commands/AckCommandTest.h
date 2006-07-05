#ifndef _ACTIVEMQ_CONNECTOR_STOMP_COMMANDS_ACKCOMMANDTEST_H_
#define _ACTIVEMQ_CONNECTOR_STOMP_COMMANDS_ACKCOMMANDTEST_H_

#include <cppunit/TestFixture.h>
#include <cppunit/extensions/HelperMacros.h>

#include <activemq/connector/stomp/commands/AckCommand.h>

namespace activemq{
namespace connector{
namespace stomp{
namespace commands{
            
    class AckCommandTest : public CppUnit::TestFixture
    {
        CPPUNIT_TEST_SUITE( AckCommandTest );
        CPPUNIT_TEST( test );
        CPPUNIT_TEST_SUITE_END();

    public:

    	AckCommandTest() {}
    	virtual ~AckCommandTest() {}

        void test(void)
        {
            AckCommand cmd;

            CPPUNIT_ASSERT( cmd.getStompCommandId() == 
                            CommandConstants::ACK );
            
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
            CPPUNIT_ASSERT( cmd.getMessageId() == NULL );
            cmd.setMessageId( "ID:123456789" );
            CPPUNIT_ASSERT( std::string( cmd.getMessageId() ) == "ID:123456789" );
            
            StompFrame* frame = cmd.marshal().clone();
            
            CPPUNIT_ASSERT( frame != NULL );
            
            AckCommand cmd1( frame );
            
            CPPUNIT_ASSERT( cmd.getCommandId() == cmd1.getCommandId() );
            CPPUNIT_ASSERT( cmd.getStompCommandId() == cmd1.getStompCommandId() );
            CPPUNIT_ASSERT( cmd.isResponseRequired() == cmd1.isResponseRequired() );
            CPPUNIT_ASSERT( cmd.getCorrelationId() == cmd1.getCorrelationId() );
            CPPUNIT_ASSERT( std::string(cmd.getMessageId()) == cmd1.getMessageId() );
            CPPUNIT_ASSERT( std::string(cmd.getTransactionId()) == cmd1.getTransactionId() );
            
        }
    };

}}}}

#endif /*_ACTIVEMQ_CONNECTOR_STOMP_COMMANDS_ACKCOMMANDTEST_H_*/
