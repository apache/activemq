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

#ifndef _ACTIVEMQ_CONNECTOR_STOMP_STOMPFRAMETEST_H_
#define _ACTIVEMQ_CONNECTOR_STOMP_STOMPFRAMETEST_H_

#include <cppunit/TestFixture.h>
#include <cppunit/extensions/HelperMacros.h>

#include <activemq/connector/stomp/StompFrame.h>

namespace activemq{
namespace connector{
namespace stomp{

   class StompFrameTest : public CppUnit::TestFixture
   {

     CPPUNIT_TEST_SUITE( StompFrameTest );
     CPPUNIT_TEST( test );
     CPPUNIT_TEST_SUITE_END();

   public:

   	virtual ~StompFrameTest() {}

      void test()
      {
         StompFrame frame;
         
         CPPUNIT_ASSERT( frame.getCommand() == "" );
         frame.setCommand("test");
         CPPUNIT_ASSERT( frame.getCommand() == "test" );
                  
         frame.getProperties().setProperty("key", "value");
         
         std::string result = frame.getProperties().getProperty("key");
         
         CPPUNIT_ASSERT( result == "value" );         
         
         CPPUNIT_ASSERT( frame.getBody() == NULL );
         CPPUNIT_ASSERT( frame.getBodyLength() == 0 );
         
         frame.setBody( strdup("ABC"), 4 );
         
         CPPUNIT_ASSERT( frame.getBody() != NULL );
         CPPUNIT_ASSERT( frame.getBodyLength() == 4 );
         CPPUNIT_ASSERT( std::string(frame.getBody()) == "ABC" );
      }
      
   };

}}}

#endif /*_ACTIVEMQ_CONNECTOR_STOMP_STOMPFRAMETEST_H_*/
