#ifndef _ACTIVEMQ_CONNECTOR_STOMP_STOMPCOMMANDWRITERTEST_H_
#define _ACTIVEMQ_CONNECTOR_STOMP_STOMPCOMMANDWRITERTEST_H_

#include <cppunit/TestFixture.h>
#include <cppunit/extensions/HelperMacros.h>

#include <activemq/connector/stomp/StompCommandWriter.h>
#include <activemq/transport/Command.h>
#include <activemq/connector/stomp/commands/ConnectedCommand.h>
#include <activemq/connector/stomp/commands/TextMessageCommand.h>
#include <activemq/connector/stomp/commands/BytesMessageCommand.h>
#include <activemq/connector/stomp/StompTopic.h>

#include <activemq/io/ByteArrayOutputStream.h>
#include <algorithm>

namespace activemq{
namespace connector{
namespace stomp{

    class StompCommandWriterTest : public CppUnit::TestFixture
    {
        CPPUNIT_TEST_SUITE( StompCommandWriterTest );
        CPPUNIT_TEST( test );
        CPPUNIT_TEST_SUITE_END();

    public:
    
    	StompCommandWriterTest() {}
    	virtual ~StompCommandWriterTest() {}
    
        void test( void )
        {
            io::ByteArrayOutputStream boStream;

            StompCommandWriter writer( &boStream );

            const char* result = 
                "CONNECTED\nsession:test\n\n\0\n"   // 26 = 26
                "SEND\n"                            // 5
                "destination:/topic/a\n"            // 21
                "message-id:123\n"                  // 15
                "sampleProperty:testvalue\n\n"      // 26
                "testMessage\0\n"                   // 13 = 80
                "SEND\n"                            // 5
                "content-length:9\n"                // 17
                "destination:/topic/a\n"            // 21
                "message-id:123\n"                  // 15
                "sampleProperty:testvalue\n\n"      // 26
                "123456789\0\n";                    // 11 = 95
                                                    //      201
            commands::ConnectedCommand connectedCommand;
            commands::TextMessageCommand textCommand;
            commands::BytesMessageCommand bytesCommand;
            
            // Sync to expected output
            connectedCommand.setSessionId( "test" );

            // Sync to expected output
            StompTopic topic1("a");
            textCommand.setCMSDestination( &topic1 );
            textCommand.setCMSMessageId( "123" );
            textCommand.getProperties().setProperty( 
                "sampleProperty", "testvalue" );
            textCommand.setText( "testMessage" );

            // Sync to expected output
            StompTopic topic2("a");
            bytesCommand.setCMSDestination( &topic2 );
            bytesCommand.setCMSMessageId( "123" );
            bytesCommand.getProperties().setProperty( 
                "sampleProperty", "testvalue" );
            bytesCommand.setBodyBytes( 
                (const unsigned char*)"123456789", 9 );

            writer.writeCommand( &connectedCommand );
            writer.writeCommand( &textCommand );
            writer.writeCommand( &bytesCommand );

            const unsigned char* alloc = boStream.getByteArray();

            //for( int i = 0; i < 201; ++i )
            //{
            //    std::cout << result[i] << " == " << alloc[i] << std::endl;
            //}

            CPPUNIT_ASSERT( boStream.getByteArraySize() == 201 );

            for( int i = 0; i < 201; ++i )
            {
                CPPUNIT_ASSERT( result[i] == alloc[i] );
            }

            // Use STL Compare
            CPPUNIT_ASSERT( 
                std::equal( &result[0], &result[200], 
                            boStream.getByteArray() ) );
        }

    };

}}}

#endif /*_ACTIVEMQ_CONNECTOR_STOMP_STOMPCOMMANDWRITERTEST_H_*/
