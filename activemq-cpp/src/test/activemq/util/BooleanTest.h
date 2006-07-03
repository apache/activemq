#ifndef _ACTIVEMQ_UTIL_BOOLEANTEST_H_
#define _ACTIVEMQ_UTIL_BOOLEANTEST_H_

#include <cppunit/TestFixture.h>
#include <cppunit/extensions/HelperMacros.h>

#include <activemq/util/Boolean.h>

namespace activemq{
namespace util{

    class BooleanTest : public CppUnit::TestFixture
    {
        CPPUNIT_TEST_SUITE( BooleanTest );
        CPPUNIT_TEST( test );
        CPPUNIT_TEST_SUITE_END();

    public:

    	BooleanTest() {}
    	virtual ~BooleanTest() {}

        virtual void test(void)
        {
            bool x = Boolean::parseBoolean("false");
            bool y = Boolean::parseBoolean("true");
            bool z = Boolean::parseBoolean("false");
            
            CPPUNIT_ASSERT( x == false );
            CPPUNIT_ASSERT( y == true );
            CPPUNIT_ASSERT( z == false );
            
            std::string x1 = Boolean::toString( x );
            std::string y1 = Boolean::toString( y );
            std::string z1 = Boolean::toString( z );

            CPPUNIT_ASSERT( x1 == "false" );
            CPPUNIT_ASSERT( y1 == "true" );
            CPPUNIT_ASSERT( z1 == "false" );

        }

    };

}}

#endif /*_ACTIVEMQ_UTIL_BOOLEANTEST_H_*/
