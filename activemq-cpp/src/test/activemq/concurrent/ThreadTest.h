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

#ifndef ACTIVEMQ_CONCURRENT_THREADTEST_H_
#define ACTIVEMQ_CONCURRENT_THREADTEST_H_

#include <cppunit/TestFixture.h>
#include <cppunit/extensions/HelperMacros.h>

#include <activemq/concurrent/Thread.h>
#include <time.h>

namespace activemq{
namespace concurrent{
	
	class ThreadTest : public CppUnit::TestFixture {
		
	  CPPUNIT_TEST_SUITE( ThreadTest );
	  CPPUNIT_TEST( testDelegate );
	  CPPUNIT_TEST( testDerived );
	  CPPUNIT_TEST( testJoin );
	  CPPUNIT_TEST_SUITE_END();
	  
	public:
	
		class Delegate : public Runnable{
		private:
		
			int stuff;
			
		public:
		
			Delegate(){ stuff = 0; }
			virtual ~Delegate(){}
			
			int getStuff(){
				return stuff;
			}
			
			virtual void run(){
				stuff = 1;
			}
		
		};
		
		class Derived : public Thread{
		private:
		
			int stuff;
			
		public:
		
			Derived(){ stuff = 0; }
			virtual ~Derived(){}
			
			int getStuff(){
				return stuff;
			}
			
			virtual void run(){
				stuff = 1;
			}
		
		};
		
		class JoinTest : public Thread{
		public:
		
			JoinTest(){}
			virtual ~JoinTest(){}
			
			virtual void run(){
				
				// Sleep for 2 seconds.
				Thread::sleep( 2000 );
			}
		
		};
		
	public:
	
		virtual void setUp(){};	
	 	virtual void tearDown(){};
		void testDelegate(){
		  	
		  	Delegate test;
		  	int initialValue = test.getStuff();
		  	
		  	Thread thread( &test );
		  	thread.start();
		  	thread.join();
		  	
		  	int finalValue = test.getStuff();
		  	
		  	// The values should be different - this proves
		  	// that the runnable was run.
		  	CPPUNIT_ASSERT( finalValue != initialValue );
		}
		
		void testDerived(){
		  	
		  	Derived test;
		  	int initialValue = test.getStuff();
		  	
		  	test.start();
		  	test.join();
		  	
		  	int finalValue = test.getStuff();
		  	
		  	// The values should be different - this proves
		  	// that the runnable was run.
		  	CPPUNIT_ASSERT( finalValue != initialValue );
		}
		
		void testJoin(){
		  	
		  	JoinTest test;
		  	
		  	time_t startTime = time( NULL );
		  	test.start();
		  	test.join();
		  	time_t endTime = time( NULL );
		  	
		  	long delta = endTime - startTime;
		  	
		  	// Should be about 5 seconds that elapsed.
		  	CPPUNIT_ASSERT( delta >= 1 && delta <= 3 );
		}
	};
	
}}

#endif /*ACTIVEMQ_CONCURRENT_THREADTEST_H_*/
