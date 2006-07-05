#ifndef _ACTIVEMQ_CONNECTOR_STOMP_COMMAND_RECEIPTCOMMANDTEST_H_
#define _ACTIVEMQ_CONNECTOR_STOMP_COMMAND_RECEIPTCOMMANDTEST_H_

#include <cppunit/TestFixture.h>
#include <cppunit/extensions/HelperMacros.h>

#include <activemq/connector/stomp/commands/ReceiptCommand.h>

namespace activemq{
namespace connector{
namespace stomp{
namespace commands{

    class ReceiptCommandTest : public CppUnit::TestFixture
    {
        CPPUNIT_TEST_SUITE( ReceiptCommandTest );
        CPPUNIT_TEST( test );
        CPPUNIT_TEST_SUITE_END();
        
    public:

    	ReceiptCommandTest() {}
    	virtual ~ReceiptCommandTest() {}

        void test(void)
        {
            ReceiptCommand cmd;

            CPPUNIT_ASSERT( cmd.getStompCommandId() == 
                            CommandConstants::RECEIPT );
            
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
            CPPUNIT_ASSERT( cmd.getReceiptId() == NULL );
            cmd.setReceiptId( "456987" );
            CPPUNIT_ASSERT( std::string( cmd.getReceiptId() ) == 
                            "456987" );
            
            StompFrame* frame = cmd.marshal().clone();
            
            CPPUNIT_ASSERT( frame != NULL );
            
            ReceiptCommand cmd1( frame );
            
            CPPUNIT_ASSERT( cmd.getCommandId() == cmd1.getCommandId() );
            CPPUNIT_ASSERT( cmd.getStompCommandId() == cmd1.getStompCommandId() );
            CPPUNIT_ASSERT( cmd.isResponseRequired() == cmd1.isResponseRequired() );
            CPPUNIT_ASSERT( cmd.getCorrelationId() == cmd1.getCorrelationId() );
            CPPUNIT_ASSERT( std::string( cmd.getTransactionId() ) == 
                            cmd1.getTransactionId() );
            CPPUNIT_ASSERT( std::string( cmd.getReceiptId() ) == 
                            cmd1.getReceiptId() );
            
        }

    };

}}}}

#endif /*_ACTIVEMQ_CONNECTOR_STOMP_COMMAND_RECEIPTCOMMANDTEST_H_*/
