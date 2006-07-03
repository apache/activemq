#ifndef _INTEGRATION_TRANSACTIONAL_TRANSACTIONTESTER_H_
#define _INTEGRATION_TRANSACTIONAL_TRANSACTIONTESTER_H_

#include <integration/common/AbstractTester.h>

#include <cppunit/TestFixture.h>
#include <cppunit/extensions/HelperMacros.h>

namespace integration{
namespace transactional{

    class TransactionTester : public CppUnit::TestFixture,
                              public common::AbstractTester
    {
        CPPUNIT_TEST_SUITE( TransactionTester );
        CPPUNIT_TEST( test );
        CPPUNIT_TEST_SUITE_END();

    public:

    	TransactionTester();
    	virtual ~TransactionTester();

        virtual void test(void);

    private:
        
    };

}}

#endif /*_INTEGRATION_TRANSACTIONAL_TRANSACTIONTESTER_H_*/
