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

#include "TransactionTester.h"
#include <integration/common/IntegrationCommon.h>

CPPUNIT_TEST_SUITE_REGISTRATION( integration::transactional::TransactionTester );

#include <activemq/concurrent/Thread.h>
#include <activemq/connector/stomp/StompConnector.h>
#include <activemq/util/SimpleProperties.h>
#include <activemq/transport/TransportFactory.h>
#include <activemq/util/Guid.h>
#include <activemq/util/SimpleProperties.h>
#include <activemq/util/StringTokenizer.h>
#include <activemq/connector/ConnectorFactoryMap.h>
#include <activemq/network/SocketFactory.h>
#include <activemq/transport/TransportFactory.h>
#include <activemq/network/Socket.h>
#include <activemq/exceptions/NullPointerException.h>
#include <activemq/core/ActiveMQConnection.h>
#include <activemq/core/ActiveMQConsumer.h>
#include <activemq/core/ActiveMQProducer.h>
#include <activemq/util/StringTokenizer.h>
#include <activemq/util/Boolean.h>

#include <cms/Connection.h>
#include <cms/MessageConsumer.h>
#include <cms/MessageProducer.h>
#include <cms/MessageListener.h>
#include <cms/Startable.h>
#include <cms/Closeable.h>
#include <cms/MessageListener.h>
#include <cms/ExceptionListener.h>
#include <cms/Topic.h>
#include <cms/Queue.h>
#include <cms/TemporaryTopic.h>
#include <cms/TemporaryQueue.h>
#include <cms/Session.h>
#include <cms/BytesMessage.h>
#include <cms/TextMessage.h>
#include <cms/MapMessage.h>

using namespace activemq::connector::stomp;
using namespace activemq::transport;
using namespace activemq::util;
using namespace std;
using namespace cms;
using namespace activemq;
using namespace activemq::core;
using namespace activemq::util;
using namespace activemq::connector;
using namespace activemq::exceptions;
using namespace activemq::network;
using namespace activemq::transport;
using namespace activemq::concurrent;

using namespace integration;
using namespace integration::transactional;
using namespace integration::common;

TransactionTester::TransactionTester() : AbstractTester( cms::Session::SESSION_TRANSACTED )
{}

TransactionTester::~TransactionTester()
{}

void TransactionTester::test()
{
    try
    {
        cout << "Starting activemqcms transactional test (sending "
             << IntegrationCommon::defaultMsgCount
             << " messages per type and sleeping "
             << IntegrationCommon::defaultDelay 
             << " milli-seconds) ...\n"
             << endl;
        
        // Create CMS Object for Comms
        cms::Topic* topic = session->createTopic("mytopic");
        cms::MessageConsumer* consumer = 
            session->createConsumer( topic );            
        consumer->setMessageListener( this );
        cms::MessageProducer* producer = 
            session->createProducer( topic );

        // Send some text messages
        this->produceTextMessages( 
            *producer, IntegrationCommon::defaultMsgCount );
            
        session->commit();
        
        // Send some bytes messages.
        this->produceTextMessages( 
            *producer, IntegrationCommon::defaultMsgCount );
        
        session->commit();

        // Wait till we get all the messages
        waitForMessages( IntegrationCommon::defaultMsgCount * 2 );
        
        printf("received: %d\n", numReceived );
        CPPUNIT_ASSERT( 
            numReceived == IntegrationCommon::defaultMsgCount * 2 );

        numReceived = 0;

        // Send some text messages
        this->produceTextMessages( 
            *producer, IntegrationCommon::defaultMsgCount );

        session->rollback();

        // Wait till we get all the messages
        waitForMessages( IntegrationCommon::defaultMsgCount );

        printf("received: %d\n", numReceived );
        CPPUNIT_ASSERT( 
            numReceived == IntegrationCommon::defaultMsgCount );

        printf("Shutting Down\n" );
        delete producer;                      
        delete consumer;
    }
    AMQ_CATCH_RETHROW( ActiveMQException )
    AMQ_CATCHALL_THROW( ActiveMQException )
}

