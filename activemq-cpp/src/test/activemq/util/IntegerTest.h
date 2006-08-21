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

#ifndef INTEGERTEST_H_
#define INTEGERTEST_H_

#include <cppunit/TestFixture.h>
#include <cppunit/extensions/HelperMacros.h>

#include <activemq/util/Integer.h>

namespace activemq{
namespace util{

    class IntegerTest : public CppUnit::TestFixture
    {
        CPPUNIT_TEST_SUITE( IntegerTest );
        CPPUNIT_TEST( test );
        CPPUNIT_TEST_SUITE_END();

    public:

    	IntegerTest(void) {}
    	virtual ~IntegerTest(void) {}

        virtual void test(void)
        {
            int x = Integer::parseInt("12");
            int y = Integer::parseInt("12.1");
            int z = Integer::parseInt("42 24");
            
            CPPUNIT_ASSERT( x == 12 );
            CPPUNIT_ASSERT( y == 12 );
            CPPUNIT_ASSERT( z == 42 );
            
            std::string x1 = Integer::toString( x );
            std::string y1 = Integer::toString( y );
            std::string z1 = Integer::toString( z );

            CPPUNIT_ASSERT( x1 == "12" );
            CPPUNIT_ASSERT( y1 == "12" );
            CPPUNIT_ASSERT( z1 == "42" );

        }

    };

}}

#endif /*INTEGERTEST_H_*/
