#ifndef _INTEGRATION_TRANSACTIONAL_DURABLETESTER_H_
#define _INTEGRATION_TRANSACTIONAL_DURABLETESTER_H_

#include <integration/common/AbstractTester.h>

#include <cppunit/TestFixture.h>
#include <cppunit/extensions/HelperMacros.h>

namespace integration{
namespace durable{

    class DurableTester : public CppUnit::TestFixture,
                          public common::AbstractTester
    {
        CPPUNIT_TEST_SUITE( DurableTester );
        CPPUNIT_TEST( test );
        CPPUNIT_TEST_SUITE_END();

    public:

    	DurableTester();
    	virtual ~DurableTester();

        virtual void test(void);

    private:
        
    };

}}

#endif /*_INTEGRATION_TRANSACTIONAL_DURABLETESTER_H_*/
