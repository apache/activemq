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
