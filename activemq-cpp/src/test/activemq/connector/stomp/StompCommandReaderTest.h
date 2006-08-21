/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#ifndef _ACTIVEMQ_CONNECTOR_STOMP_STOMPCOMMANDREADERTEST_H_
#define _ACTIVEMQ_CONNECTOR_STOMP_STOMPCOMMANDREADERTEST_H_

#include <cppunit/TestFixture.h>
#include <cppunit/extensions/HelperMacros.h>

#include <activemq/connector/stomp/StompCommandReader.h>
#include <activemq/transport/Command.h>
#include <activemq/connector/stomp/commands/ConnectedCommand.h>
#include <activemq/connector/stomp/commands/TextMessageCommand.h>
#include <activemq/connector/stomp/commands/BytesMessageCommand.h>

#include <activemq/io/ByteArrayInputStream.h>

namespace activemq{
namespace connector{
namespace stomp{

    class StompCommandReaderTest : public CppUnit::TestFixture
    {
        CPPUNIT_TEST_SUITE( StompCommandReaderTest );
        CPPUNIT_TEST( test );
        CPPUNIT_TEST_SUITE_END();

    public:
    
    	StompCommandReaderTest() {}
    	virtual ~StompCommandReaderTest() {}

        void test( void )
        {
            io::ByteArrayInputStream biStream;

            StompCommandReader reader( &biStream );

            const char* connectedStr = 
                "CONNECTED\nsession:test\n\n\0\n";
            const char* textStr = 
                "MESSAGE\n"
                "destination:/topic/a\n"
                "message-id:123\n"
                "sampleProperty:testvalue\n\n"
                "testMessage\0\n";
            const char* bytesStr = 
                "MESSAGE\n"                    // 8
                "destination:/topic/a\n"       // 21
                "message-id:123\n"             // 15
                "content-length:9\n"           // 17
                "sampleProperty:testvalue\n\n" // 26
                "123456789\0\n";               // 11
            
            biStream.setByteArray( 
                (const unsigned char*)connectedStr, 27 );

            transport::Command* command = reader.readCommand();

            CPPUNIT_ASSERT( command != NULL );
            
            commands::ConnectedCommand* connected = 
                dynamic_cast< commands::ConnectedCommand* >( command );

            CPPUNIT_ASSERT( connected != NULL );

            CPPUNIT_ASSERT( connected->getSessionId() != NULL );
            std::string sessionId = connected->getSessionId();
            CPPUNIT_ASSERT( sessionId == "test" );

            biStream.setByteArray( 
                (const unsigned char*)textStr, 83 );

            delete command;
            
            command = reader.readCommand();

            CPPUNIT_ASSERT( command != NULL );
            
            commands::TextMessageCommand* textMessage = 
                dynamic_cast< commands::TextMessageCommand* >( command );

            CPPUNIT_ASSERT( textMessage != NULL );

            CPPUNIT_ASSERT( textMessage->getText() != NULL );
            std::string text = textMessage->getText();
            CPPUNIT_ASSERT( text == "testMessage" );

            biStream.setByteArray( 
                (const unsigned char*)bytesStr, 98 );

            delete command;

            command = reader.readCommand();

            CPPUNIT_ASSERT( command != NULL );
            
            commands::BytesMessageCommand* bytesMessage = 
                dynamic_cast< commands::BytesMessageCommand* >( command );

            CPPUNIT_ASSERT( bytesMessage != NULL );

            CPPUNIT_ASSERT( bytesMessage->getBodyBytes() != NULL );
            std::string bytesText( 
                (const char*)bytesMessage->getBodyBytes(), 
                (int)bytesMessage->getBodyLength() );
            CPPUNIT_ASSERT( bytesText == "123456789" );

            delete command;
        }
        
    };

}}}

#endif /*_ACTIVEMQ_CONNECTOR_STOMP_STOMPCOMMANDREADERTEST_H_*/
