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

#ifndef _ACTIVEMQ_CONNECTOR_STOMP_MARSHAL_MARSHALERTEST_H_
#define _ACTIVEMQ_CONNECTOR_STOMP_MARSHAL_MARSHALERTEST_H_

#include <cppunit/TestFixture.h>
#include <cppunit/extensions/HelperMacros.h>

#include <activemq/transport/Command.h>
#include <activemq/connector/stomp/StompTopic.h>
#include <activemq/connector/stomp/commands/ConnectedCommand.h>
#include <activemq/connector/stomp/commands/TextMessageCommand.h>
#include <activemq/connector/stomp/commands/BytesMessageCommand.h>
#include <activemq/connector/stomp/marshal/Marshaler.h>

namespace activemq{
namespace connector{
namespace stomp{
namespace marshal{

    class MarshalerTest : public CppUnit::TestFixture
    {
        CPPUNIT_TEST_SUITE( MarshalerTest );
        CPPUNIT_TEST( test );
        CPPUNIT_TEST_SUITE_END();

    public:
    
    	MarshalerTest() {}
    	virtual ~MarshalerTest() {}

        void test( void )
        {
            Marshaler marshaler;
            
            commands::ConnectedCommand   connectedCommand;
            commands::TextMessageCommand textCommand;
            commands::BytesMessageCommand bytesCommand;
            
            // Sync to expected output
            connectedCommand.setSessionId( "test" );

            StompTopic myTopic( "a" );

            // Sync to expected output
            textCommand.setCMSDestination( &myTopic );
            textCommand.setCMSMessageId( "123" );
            textCommand.getProperties().setProperty( 
                "sampleProperty", "testvalue" );
            textCommand.setText( "testMessage" );

            // Sync to expected output
            bytesCommand.setCMSDestination( &myTopic );
            bytesCommand.setCMSMessageId( "123" );
            bytesCommand.getProperties().setProperty( 
                "sampleProperty", "testvalue" );
            bytesCommand.setBodyBytes( 
                (const unsigned char*)"123456789\0", 10 );
            
            StompFrame* connectedFrame = 
                marshaler.marshal( &connectedCommand ).clone();
            StompFrame* textFrame = 
                marshaler.marshal( &textCommand ).clone();
            StompFrame* bytesFrame = 
                marshaler.marshal( &bytesCommand ).clone();

            CPPUNIT_ASSERT( connectedFrame != NULL );
            CPPUNIT_ASSERT( textFrame != NULL );
            CPPUNIT_ASSERT( bytesFrame != NULL );

            commands::ConnectedCommand   connectedCommand1( connectedFrame );
            commands::TextMessageCommand textCommand1( textFrame );
            commands::BytesMessageCommand bytesCommand1( bytesFrame );
            
            // Connected Tests
            CPPUNIT_ASSERT( connectedCommand.getCommandId() == 
                            connectedCommand1.getCommandId() );
            CPPUNIT_ASSERT( connectedCommand.getStompCommandId() == 
                            connectedCommand1.getStompCommandId() );
            CPPUNIT_ASSERT( connectedCommand.isResponseRequired() == 
                            connectedCommand1.isResponseRequired() );
            CPPUNIT_ASSERT( connectedCommand.getCorrelationId() == 
                            connectedCommand1.getCorrelationId() );

            // TextMessage Tests
            CPPUNIT_ASSERT( textCommand.getCommandId() == 
                            textCommand1.getCommandId() );
            CPPUNIT_ASSERT( textCommand.getStompCommandId() == 
                            textCommand1.getStompCommandId() );
            CPPUNIT_ASSERT( std::string( textCommand.getText() ) == 
                            textCommand1.getText() );

            // BytesMessage Tests
            CPPUNIT_ASSERT( bytesCommand.getCommandId() == 
                            bytesCommand1.getCommandId() );
            CPPUNIT_ASSERT( bytesCommand.getStompCommandId() == 
                            bytesCommand1.getStompCommandId() );
            CPPUNIT_ASSERT( std::string( (const char*)bytesCommand.getBodyBytes() ) == 
                            (const char*)bytesCommand1.getBodyBytes() );
            

        }

    };

}}}}

#endif /*_ACTIVEMQ_CONNECTOR_STOMP_MARSHAL_MARSHALERTEST_H_*/
