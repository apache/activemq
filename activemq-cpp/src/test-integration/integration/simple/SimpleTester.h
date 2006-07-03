#ifndef _INTEGRATION_SIMPLE_SIMPLETESTER_H_
#define _INTEGRATION_SIMPLE_SIMPLETESTER_H_

#include <cppunit/TestFixture.h>
#include <cppunit/extensions/HelperMacros.h>

#include <integration/common/AbstractTester.h>

namespace integration{
namespace simple{
    
    class SimpleTester : public CppUnit::TestFixture,
                         public common::AbstractTester
    {
        CPPUNIT_TEST_SUITE( SimpleTester );
        CPPUNIT_TEST( test );
        CPPUNIT_TEST_SUITE_END();

    public:

    	SimpleTester();
    	virtual ~SimpleTester();

        virtual void test(void);

    };

}}

#endif /*_INTEGRATION_SIMPLE_SIMPLETESTER_H_*/
