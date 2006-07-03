#ifndef ACTIVEMQ_EXCEPTIONS_ACTIVEMQEXCEPTIONTEST_H_
#define ACTIVEMQ_EXCEPTIONS_ACTIVEMQEXCEPTIONTEST_H_

#include <cppunit/TestFixture.h>
#include <cppunit/extensions/HelperMacros.h>

#include <activemq/exceptions/ActiveMQException.h>
#include <string.h>

namespace activemq{
namespace exceptions{
	
	class ActiveMQExceptionTest : public CppUnit::TestFixture {
		
	  CPPUNIT_TEST_SUITE( ActiveMQExceptionTest );
	  CPPUNIT_TEST( testMessage0 );
	  CPPUNIT_TEST( testMessage3 );
	  CPPUNIT_TEST_SUITE_END();
	  
	public:
	
		virtual void setUp(){};	
	 	virtual void tearDown(){};
		void testMessage0(){
		  	char* text = "This is a test";
		  	ActiveMQException ex( __FILE__, __LINE__, text );
		  	CPPUNIT_ASSERT( strcmp( ex.getMessage(), text ) == 0 );
		}
	  
	  	void testMessage3(){
	  		ActiveMQException ex( __FILE__, __LINE__, 
                "This is a test %d %d %d", 1, 100, 1000 );
	  		CPPUNIT_ASSERT( strcmp( ex.getMessage(), "This is a test 1 100 1000" ) == 0 );
	  	}
	};
	
}}

#endif /*ACTIVEMQ_EXCEPTIONS_ACTIVEMQEXCEPTIONTEST_H_*/
