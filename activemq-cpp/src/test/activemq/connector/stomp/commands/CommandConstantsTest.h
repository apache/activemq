#ifndef _ACTIVEMQ_CONNECTOR_STOMP_COMMANDS_COMMANDCONSTANTSTEST_H_
#define _ACTIVEMQ_CONNECTOR_STOMP_COMMANDS_COMMANDCONSTANTSTEST_H_

#include <cppunit/TestFixture.h>
#include <cppunit/extensions/HelperMacros.h>

#include <activemq/connector/stomp/commands/CommandConstants.h>

namespace activemq{
namespace connector{
namespace stomp{
namespace commands{

    class CommandConstantsTest : public CppUnit::TestFixture
    {
        CPPUNIT_TEST_SUITE( CommandConstantsTest );
        CPPUNIT_TEST( test );
        CPPUNIT_TEST_SUITE_END();

    public:

    	CommandConstantsTest() {}
    	virtual ~CommandConstantsTest() {}

        void test(void)
        {
            int index = 0;
            
            for(; index < CommandConstants::NUM_COMMANDS; ++index )
            {
                CommandConstants::CommandId cmdId = 
                    (CommandConstants::CommandId)index;
                std::string cmd = CommandConstants::toString( cmdId );
    
                CPPUNIT_ASSERT( cmd != "");            
                CPPUNIT_ASSERT( 
                    cmdId == CommandConstants::toCommandId( cmd ) );
            }
            
            CPPUNIT_ASSERT( index != 0 );
            
            index = 0;
                                        
            for(; index < CommandConstants::NUM_STOMP_HEADERS; ++index )
            {
                CommandConstants::StompHeader hdrId = 
                    (CommandConstants::StompHeader)index;
                std::string hdr = CommandConstants::toString( hdrId );
    
                CPPUNIT_ASSERT( hdr != "");            
                CPPUNIT_ASSERT( 
                    hdrId == CommandConstants::toStompHeader( hdr ) );
            }

            CPPUNIT_ASSERT( index != 0 );
            
            index = 0;
                                        
            for(; index < CommandConstants::NUM_ACK_MODES; ++index )
            {
                CommandConstants::AckMode ackMode = 
                    (CommandConstants::AckMode)index;
                std::string ackStr = CommandConstants::toString( ackMode );
    
                CPPUNIT_ASSERT( ackStr != "");            
                CPPUNIT_ASSERT( 
                    ackMode == CommandConstants::toAckMode( ackStr ) );
            }

            CPPUNIT_ASSERT( index != 0 );

            CPPUNIT_ASSERT( CommandConstants::queuePrefix != NULL );
            CPPUNIT_ASSERT( CommandConstants::topicPrefix != NULL );

        }

    };

}}}}

#endif /*_ACTIVEMQ_CONNECTOR_STOMP_COMMANDS_COMMANDCONSTANTSTEST_H_*/
