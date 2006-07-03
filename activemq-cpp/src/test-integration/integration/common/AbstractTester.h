#ifndef _INTEGRATION_COMMON_ABSTRACTTESTER_H_
#define _INTEGRATION_COMMON_ABSTRACTTESTER_H_

#include "Tester.h"

#include <activemq/concurrent/Mutex.h>

#include <cms/ConnectionFactory.h>
#include <cms/Connection.h>
#include <cms/Session.h>
#include <cms/MessageProducer.h>

namespace integration{
namespace common{

    class AbstractTester : public Tester
    {
    public:
    
    	AbstractTester( cms::Session::AcknowledgeMode ackMode = 
                            cms::Session::AutoAcknowledge );
    	virtual ~AbstractTester();
    
        virtual void doSleep(void);

        virtual unsigned int produceTextMessages( 
            cms::MessageProducer& producer,
            unsigned int count );
        virtual unsigned int produceBytesMessages( 
            cms::MessageProducer& producer,
            unsigned int count );

        virtual void waitForMessages( unsigned int count );

        virtual void onException( const cms::CMSException& error );
        virtual void onMessage( const cms::Message& message );

    public:
        
        cms::ConnectionFactory* connectionFactory;
        cms::Connection* connection;
        cms::Session* session;

        unsigned int numReceived;
        activemq::concurrent::Mutex mutex;

    };

}}

#endif /*_INTEGRATION_COMMON_ABSTRACTTESTER_H_*/
