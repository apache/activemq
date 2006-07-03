#ifndef _ACTIVEMQ_CONNECTOR_STOMP_COMMAND_UNSUBSCRIBECOMMANDTEST_H_
#define _ACTIVEMQ_CONNECTOR_STOMP_COMMAND_UNSUBSCRIBECOMMANDTEST_H_

#include <cppunit/TestFixture.h>
#include <cppunit/extensions/HelperMacros.h>

#include <activemq/connector/stomp/commands/UnsubscribeCommand.h>

namespace activemq{
namespace connector{
namespace stomp{
namespace commands{

    class UnsubscribeCommandTest : public CppUnit::TestFixture
    {
        CPPUNIT_TEST_SUITE( UnsubscribeCommandTest );
        CPPUNIT_TEST( test );
        CPPUNIT_TEST_SUITE_END();
        
    public:
    	UnsubscribeCommandTest() {}
    	virtual ~UnsubscribeCommandTest() {}

        void test(void)
        {
            UnsubscribeCommand cmd;

            CPPUNIT_ASSERT( cmd.getStompCommandId() == 
                            CommandConstants::UNSUBSCRIBE );
            
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
            
            StompFrame* frame = cmd.marshal().clone();
            
            CPPUNIT_ASSERT( frame != NULL );
            
            UnsubscribeCommand cmd1( frame );
            
            CPPUNIT_ASSERT( cmd.getCommandId() == cmd1.getCommandId() );
            CPPUNIT_ASSERT( cmd.getStompCommandId() == cmd1.getStompCommandId() );
            CPPUNIT_ASSERT( cmd.isResponseRequired() == cmd1.isResponseRequired() );
            CPPUNIT_ASSERT( cmd.getCorrelationId() == cmd1.getCorrelationId() );
            CPPUNIT_ASSERT( std::string( cmd.getTransactionId() ) == 
                            cmd1.getTransactionId() );
            CPPUNIT_ASSERT( std::string( cmd.getDestination() ) == 
                            cmd1.getDestination() );
            
        }
    };

}}}}

#endif /*_ACTIVEMQ_CONNECTOR_STOMP_COMMAND_UNSUBSCRIBECOMMANDTEST_H_*/
