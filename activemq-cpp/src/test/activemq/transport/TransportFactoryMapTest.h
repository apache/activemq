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

#ifndef ACTIVEMQ_TRANSPORT_TRANSPORTFACTORYMAPTEST_H_
#define ACTIVEMQ_TRANSPORT_TRANSPORTFACTORYMAPTEST_H_

#include <cppunit/TestFixture.h>
#include <cppunit/extensions/HelperMacros.h>

#include <activemq/transport/TransportFactoryMap.h>
#include <activemq/transport/TransportFactory.h>

namespace activemq{
namespace transport{
	
	class TransportFactoryMapTest : public CppUnit::TestFixture {
		
	  CPPUNIT_TEST_SUITE( TransportFactoryMapTest );
	  CPPUNIT_TEST( test );
	  CPPUNIT_TEST_SUITE_END();	  
		
	public:
	
		class TestTransportFactory : public TransportFactory
		{
		public:
		
		   virtual Transport* createTransport(
		      const activemq::util::Properties& properties) { return NULL; };
		};
		
		void test(){
			
			TransportFactoryMap& factMap = 
                TransportFactoryMap::getInstance();
			TestTransportFactory testFactory;
			
			factMap.registerTransportFactory( "test", &testFactory );
			
			CPPUNIT_ASSERT( factMap.lookup( "test" ) == &testFactory );
			
			std::vector<std::string> names;
			CPPUNIT_ASSERT( factMap.getFactoryNames( names ) >= 1 );
			
            bool found = false;
            for( unsigned int i = 0; i < names.size(); ++i )
            {
                if( names[i] == "test" )
                {
                    found = true;
                    break;
                }
            }
			CPPUNIT_ASSERT( found );
			
			factMap.unregisterTransportFactory( "test" );
			CPPUNIT_ASSERT( factMap.lookup( "test" ) == NULL );			
		}
	};
	
}}

#endif /*ACTIVEMQ_TRANSPORT_TRANSPORTFACTORYMAPTEST_H_*/
