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
