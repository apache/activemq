#ifndef COMMITCOMMANDTEST_H_
#define COMMITCOMMANDTEST_H_

#include <cppunit/TestFixture.h>
#include <cppunit/extensions/HelperMacros.h>

#include <activemq/connector/stomp/commands/CommitCommand.h>

namespace activemq{
namespace connector{
namespace stomp{
namespace commands{

    class CommitCommandTest : public CppUnit::TestFixture
    {
        CPPUNIT_TEST_SUITE( CommitCommandTest );
        CPPUNIT_TEST( test );
        CPPUNIT_TEST_SUITE_END();

    public:

    	CommitCommandTest() {}
    	virtual ~CommitCommandTest() {}

        void test(void)
        {
            CommitCommand cmd;

            CPPUNIT_ASSERT( cmd.getStompCommandId() == 
                            CommandConstants::COMMIT );
            
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
            
            CommitCommand cmd1( frame );
            
            CPPUNIT_ASSERT( cmd.getCommandId() == cmd1.getCommandId() );
            CPPUNIT_ASSERT( cmd.getStompCommandId() == cmd1.getStompCommandId() );
            CPPUNIT_ASSERT( cmd.isResponseRequired() == cmd1.isResponseRequired() );
            CPPUNIT_ASSERT( cmd.getCorrelationId() == cmd1.getCorrelationId() );
            CPPUNIT_ASSERT( std::string(cmd.getTransactionId()) == cmd1.getTransactionId() );
            
        }

    };

}}}}

#endif /*COMMITCOMMANDTEST_H_*/
