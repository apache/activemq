#ifndef _ACTIVEMQ_CONNECTOR_STOMP_COMMANDS_CONNECTCOMMANDTEST_H_
#define _ACTIVEMQ_CONNECTOR_STOMP_COMMANDS_CONNECTCOMMANDTEST_H_

#include <cppunit/TestFixture.h>
#include <cppunit/extensions/HelperMacros.h>

#include <activemq/connector/stomp/commands/ConnectCommand.h>

namespace activemq{
namespace connector{
namespace stomp{
namespace commands{

    class ConnectCommandTest : public CppUnit::TestFixture
    {
        CPPUNIT_TEST_SUITE( ConnectCommandTest );
        CPPUNIT_TEST( test );
        CPPUNIT_TEST_SUITE_END();

    public:

    	ConnectCommandTest() {}
    	virtual ~ConnectCommandTest() {}

        void test(void)
        {
            ConnectCommand cmd;

            CPPUNIT_ASSERT( cmd.getStompCommandId() == 
                            CommandConstants::CONNECT );
            
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
            
            StompFrame* frame = cmd.marshal().clone();
            
            CPPUNIT_ASSERT( frame != NULL );
            
            ConnectCommand cmd1( frame );
            
            CPPUNIT_ASSERT( cmd.getCommandId() == cmd1.getCommandId() );
            CPPUNIT_ASSERT( cmd.getStompCommandId() == cmd1.getStompCommandId() );
            CPPUNIT_ASSERT( cmd.isResponseRequired() == cmd1.isResponseRequired() );
            CPPUNIT_ASSERT( cmd.getCorrelationId() == cmd1.getCorrelationId() );
            CPPUNIT_ASSERT( std::string(cmd.getTransactionId()) == cmd1.getTransactionId() );
            
        }

    };

}}}}

#endif /*_ACTIVEMQ_CONNECTOR_STOMP_COMMANDS_CONNECTCOMMANDTEST_H_*/
