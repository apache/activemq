#ifndef _ACTIVEMQ_CORE_ACTIVEMQCONNECTIONFACTORYTEST_H_
#define _ACTIVEMQ_CORE_ACTIVEMQCONNECTIONFACTORYTEST_H_

#include <cppunit/TestFixture.h>
#include <cppunit/extensions/HelperMacros.h>

#include <activemq/network/Socket.h>
#include <activemq/network/ServerSocket.h>
#include <activemq/concurrent/Concurrent.h>
#include <activemq/concurrent/Mutex.h>
#include <activemq/concurrent/Thread.h>
#include <activemq/core/ActiveMQConnectionFactory.h>
#include <cms/Connection.h>
#include <activemq/transport/TransportFactoryMapRegistrar.h>
#include <activemq/transport/DummyTransportFactory.h>

namespace activemq{
namespace core{

    class ActiveMQConnectionFactoryTest : public CppUnit::TestFixture
    {
        CPPUNIT_TEST_SUITE( ActiveMQConnectionFactoryTest );
        CPPUNIT_TEST( test );
        CPPUNIT_TEST_SUITE_END();

    public:
    
    	ActiveMQConnectionFactoryTest() {}
    	virtual ~ActiveMQConnectionFactoryTest() {}
        
        void test()
        {
            try
            {
                transport::TransportFactoryMapRegistrar registrar(
                    "dummy", new transport::DummyTransportFactory() );
                    
                std::string URI = 
                    "dummy://127.0.0.1:23232&wireFormat=stomp";

                ActiveMQConnectionFactory connectionFactory(URI);

                cms::Connection* connection = 

                connectionFactory.createConnection();

                CPPUNIT_ASSERT( connection != NULL );
                
                delete connection;
                
                return;
            }
            AMQ_CATCH_NOTHROW( exceptions::ActiveMQException )
            AMQ_CATCHALL_NOTHROW( )
            
            CPPUNIT_ASSERT( false );
        }
        
    };
    
}}

#endif /*_ACTIVEMQ_CORE_ACTIVEMQCONNECTIONFACTORYTEST_H_*/
