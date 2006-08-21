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

#ifndef _ACTIVEMQ_UTIL_LONGTEST_H_
#define _ACTIVEMQ_UTIL_LONGTEST_H_

#include <cppunit/TestFixture.h>
#include <cppunit/extensions/HelperMacros.h>

#include <activemq/util/Long.h>

namespace activemq{
namespace util{

    class LongTest : public CppUnit::TestFixture
    {
        CPPUNIT_TEST_SUITE( LongTest );
        CPPUNIT_TEST( test );
        CPPUNIT_TEST_SUITE_END();

    public:

    	LongTest() {}
    	virtual ~LongTest() {}

        virtual void test(void)
        {
            long x = Long::parseLong("12");
            long y = Long::parseLong("12.1");
            long z = Long::parseLong("42 24");
            
            CPPUNIT_ASSERT( x == 12 );
            CPPUNIT_ASSERT( y == 12 );
            CPPUNIT_ASSERT( z == 42 );
            
            std::string x1 = Long::toString( x );
            std::string y1 = Long::toString( y );
            std::string z1 = Long::toString( z );

            CPPUNIT_ASSERT( x1 == "12" );
            CPPUNIT_ASSERT( y1 == "12" );
            CPPUNIT_ASSERT( z1 == "42" );

        }

    };

}}

#endif /*_ACTIVEMQ_UTIL_LONGTEST_H_*/
