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

#ifndef _ACTIVEMQ_UTIL_STRINGTOKENIZERTEST_H_
#define _ACTIVEMQ_UTIL_STRINGTOKENIZERTEST_H_

#include <cppunit/TestFixture.h>
#include <cppunit/extensions/HelperMacros.h>

#include <activemq/util/StringTokenizer.h>

namespace activemq{
namespace util{

   class StringTokenizerTest : public CppUnit::TestFixture 
   {
     CPPUNIT_TEST_SUITE( StringTokenizerTest );
     CPPUNIT_TEST( test );
     CPPUNIT_TEST_SUITE_END();
   public:

   	virtual ~StringTokenizerTest() {}

      void test()
      {
         StringTokenizer tokenizer("stomp://127.0.0.1:23232", "://");
         CPPUNIT_ASSERT( tokenizer.countTokens() == 3 );
         CPPUNIT_ASSERT( tokenizer.nextToken() == "stomp" );
         CPPUNIT_ASSERT( tokenizer.nextToken() == "127.0.0.1" );
         CPPUNIT_ASSERT( tokenizer.nextToken() == "23232" );

         StringTokenizer tokenizer1("::://stomp://127.0.0.1:23232:", ":/");
         CPPUNIT_ASSERT( tokenizer1.countTokens() == 3 );
         CPPUNIT_ASSERT( tokenizer1.nextToken() == "stomp" );
         CPPUNIT_ASSERT( tokenizer1.nextToken() == "127.0.0.1" );
         CPPUNIT_ASSERT( tokenizer1.nextToken() == "23232" );

         StringTokenizer tokenizer2("test");
         CPPUNIT_ASSERT( tokenizer2.countTokens() == 1 );
         CPPUNIT_ASSERT( tokenizer2.hasMoreTokens() == true );
         CPPUNIT_ASSERT( tokenizer2.nextToken() == "test" );
         CPPUNIT_ASSERT( tokenizer2.hasMoreTokens() == false );

         StringTokenizer tokenizer3(":", ":");
         CPPUNIT_ASSERT( tokenizer3.countTokens() == 0 );
         CPPUNIT_ASSERT( tokenizer3.hasMoreTokens() == false );
         CPPUNIT_ASSERT( tokenizer3.nextToken(" ") == ":" );

         try
         {
            tokenizer3.nextToken();

            CPPUNIT_ASSERT( false );
         }
         catch(exceptions::NoSuchElementException ex)
         {
            CPPUNIT_ASSERT( true );
         }

         StringTokenizer tokenizer4("the quick brown fox");
         CPPUNIT_ASSERT( tokenizer4.countTokens() == 4 );
         CPPUNIT_ASSERT( tokenizer4.hasMoreTokens() == true );
         CPPUNIT_ASSERT( tokenizer4.nextToken() == "the" );
         CPPUNIT_ASSERT( tokenizer4.nextToken() == "quick" );
         CPPUNIT_ASSERT( tokenizer4.nextToken() == "brown" );
         CPPUNIT_ASSERT( tokenizer4.nextToken() == "fox" );
         CPPUNIT_ASSERT( tokenizer4.countTokens() == 0 );
         CPPUNIT_ASSERT( tokenizer4.hasMoreTokens() == false );

         StringTokenizer tokenizer5("the:quick:brown:fox", ":", true);
         CPPUNIT_ASSERT( tokenizer5.countTokens() == 7 );
         CPPUNIT_ASSERT( tokenizer5.hasMoreTokens() == true );
         CPPUNIT_ASSERT( tokenizer5.nextToken() == "the" );
         CPPUNIT_ASSERT( tokenizer5.nextToken() == ":" );
         CPPUNIT_ASSERT( tokenizer5.nextToken() == "quick" );
         CPPUNIT_ASSERT( tokenizer5.nextToken() == ":" );
         CPPUNIT_ASSERT( tokenizer5.nextToken() == "brown" );
         CPPUNIT_ASSERT( tokenizer5.nextToken() == ":" );
         CPPUNIT_ASSERT( tokenizer5.nextToken() == "fox" );
         CPPUNIT_ASSERT( tokenizer5.countTokens() == 0 );
         CPPUNIT_ASSERT( tokenizer5.hasMoreTokens() == false );

         std::vector<std::string> myArray;
         StringTokenizer tokenizer6("the:quick:brown:fox", ":");
         CPPUNIT_ASSERT( tokenizer6.countTokens() == 4 );
         CPPUNIT_ASSERT( tokenizer6.toArray(myArray) == 4 );
         CPPUNIT_ASSERT( tokenizer6.countTokens() == 0 );
         tokenizer6.reset();
         CPPUNIT_ASSERT( tokenizer6.countTokens() == 4 );
         tokenizer6.reset("the:quick:brown:fox", "$");
         CPPUNIT_ASSERT( tokenizer6.countTokens() == 1 );
         tokenizer6.reset("this$is$a$test");
         CPPUNIT_ASSERT( tokenizer6.countTokens() == 4 );
         tokenizer6.reset("this$is$a$test", "$", true);
         CPPUNIT_ASSERT( tokenizer6.countTokens() == 7 );

      }
   };

}}

#endif /*_ACTIVEMQ_UTIL_STRINGTOKENIZERTEST_H_*/
