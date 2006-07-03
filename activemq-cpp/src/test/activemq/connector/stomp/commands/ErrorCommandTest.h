#ifndef _ACTIVEMQ_CONNECTOR_STOMP_COMMANDS_ERRORCOMMANDTEST_H_
#define _ACTIVEMQ_CONNECTOR_STOMP_COMMANDS_ERRORCOMMANDTEST_H_

#include <cppunit/TestFixture.h>
#include <cppunit/extensions/HelperMacros.h>

#include <activemq/connector/stomp/commands/ErrorCommand.h>

namespace activemq{
namespace connector{
namespace stomp{
namespace commands{

    class ErrorCommandTest : public CppUnit::TestFixture
    {
        CPPUNIT_TEST_SUITE( ErrorCommandTest );
        CPPUNIT_TEST( test );
        CPPUNIT_TEST_SUITE_END();
        
    public:

    	ErrorCommandTest() {}
    	virtual ~ErrorCommandTest() {}

        void test(void)
        {
            ErrorCommand cmd;

            CPPUNIT_ASSERT( cmd.getStompCommandId() == 
                            CommandConstants::ERROR_CMD );
            
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
            CPPUNIT_ASSERT( cmd.getErrorMessage() == NULL );
            cmd.setErrorMessage( "Error" );
            CPPUNIT_ASSERT( std::string( cmd.getErrorMessage() ) == "Error" );
            CPPUNIT_ASSERT( cmd.getErrorDetails() == NULL );
            cmd.setErrorDetails( "ErrorD" );
            CPPUNIT_ASSERT( std::string( cmd.getErrorDetails() ) == "ErrorD" );
            
            StompFrame* frame = cmd.marshal().clone();
            
            CPPUNIT_ASSERT( frame != NULL );
            
            ErrorCommand cmd1( frame );
            
            CPPUNIT_ASSERT( cmd.getCommandId() == cmd1.getCommandId() );
            CPPUNIT_ASSERT( cmd.getStompCommandId() == cmd1.getStompCommandId() );
            CPPUNIT_ASSERT( cmd.isResponseRequired() == cmd1.isResponseRequired() );
            CPPUNIT_ASSERT( cmd.getCorrelationId() == cmd1.getCorrelationId() );
            CPPUNIT_ASSERT( std::string(cmd.getTransactionId()) == cmd1.getTransactionId() );
            CPPUNIT_ASSERT( std::string(cmd.getErrorMessage()) == cmd1.getErrorMessage() );
            CPPUNIT_ASSERT( std::string(cmd.getErrorDetails()) == cmd1.getErrorDetails() );
            
        }

    };

}}}}

#endif /*_ACTIVEMQ_CONNECTOR_STOMP_COMMANDS_ERRORCOMMANDTEST_H_*/
