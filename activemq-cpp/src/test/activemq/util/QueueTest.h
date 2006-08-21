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

#ifndef _ACTIVEMQ_UTIL_QUEUETEST_H_
#define _ACTIVEMQ_UTIL_QUEUETEST_H_

#include <cppunit/TestFixture.h>
#include <cppunit/extensions/HelperMacros.h>

#include <activemq/util/Queue.h>

namespace activemq{
namespace util{

   class QueueTest : public CppUnit::TestFixture {

     CPPUNIT_TEST_SUITE( QueueTest );
     CPPUNIT_TEST( test );
     CPPUNIT_TEST_SUITE_END();

   public:

   	virtual ~QueueTest() {}
    
      void test()
      {
         Queue<char> q;
         
         CPPUNIT_ASSERT( q.empty() == true );
         CPPUNIT_ASSERT( q.size() == 0 );

         q.push('a');
    
         CPPUNIT_ASSERT( q.front() == 'a' );
         
         q.pop();
         
         CPPUNIT_ASSERT( q.empty() == true );
         
         q.push('b');
         q.push('c');
         
         CPPUNIT_ASSERT( q.size() == 2 );
         
         CPPUNIT_ASSERT( q.front() == 'b' );
         CPPUNIT_ASSERT( q.back() == 'c' );

      }
   };
    
}}

#endif /*_ACTIVEMQ_UTIL_QUEUETEST_H_*/
