#ifndef _INTEGRATION_VARIOUS_SIMPLEROLLBACKTEST_H_
#define _INTEGRATION_VARIOUS_SIMPLEROLLBACKTEST_H_

#include <cppunit/TestFixture.h>
#include <cppunit/extensions/HelperMacros.h>

#include <activemq/concurrent/Mutex.h>

#include <cms/MessageListener.h>
#include <cms/ExceptionListener.h>
#include <cms/ConnectionFactory.h>
#include <cms/Connection.h>
#include <cms/Session.h>
#include <cms/MessageProducer.h>

namespace integration{
namespace various{

    class SimpleRollbackTest : public CppUnit::TestFixture,
                               public cms::ExceptionListener,
                               public cms::MessageListener    
    {
        CPPUNIT_TEST_SUITE( SimpleRollbackTest );
        CPPUNIT_TEST( test );
        CPPUNIT_TEST_SUITE_END();

    public:
    
        SimpleRollbackTest();
        virtual ~SimpleRollbackTest();
        
        virtual void test(void);
        
        virtual void onException( const cms::CMSException& error );
        virtual void onMessage( const cms::Message* message );

    private:

        cms::ConnectionFactory* connectionFactory;
        cms::Connection* connection;
        cms::Session* session;

        unsigned int numReceived;
        unsigned int msgCount;
        activemq::concurrent::Mutex mutex;

    };

}}

#endif /*_INTEGRATION_VARIOUS_SIMPLEROLLBACKTEST_H_*/
