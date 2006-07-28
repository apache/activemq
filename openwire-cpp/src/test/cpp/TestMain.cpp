/*
 * Copyright 2006 The Apache Software Foundation or its licensors, as
 * applicable.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
#include <string>

#include "cms/IConnectionFactory.hpp"
#include "cms/IConnection.hpp"
#include "cms/IDestination.hpp"
#include "cms/IMessageConsumer.hpp"
#include "cms/IMessageProducer.hpp"
#include "cms/ISession.hpp"
#include "cms/ITextMessage.hpp"
#include "activemq/ConnectionFactory.hpp"
#include "activemq/Connection.hpp"
#include "activemq/command/ActiveMQTextMessage.hpp"
#include "ppr/TraceException.hpp"
#include "ppr/util/MapItemHolder.hpp"
#include "ppr/net/Uri.hpp"
#include "ppr/util/ifr/p"
#include "TestListener.hpp"

using namespace apache::activemq;
using namespace apache::activemq::command;
using namespace apache::cms;
using namespace apache::ppr;
using namespace apache::ppr::net;
using namespace apache::ppr::util;
using namespace ifr;
using namespace std;

/*
 * Tests synchronous sending/receiving of a text message
 */
void testSyncTextMessage()
{
    try
    {
        p<IConnectionFactory> factory ;
        p<IConnection>        connection ;
        p<ISession>           session ;
        p<IQueue>             queue ;
        p<IMessageConsumer>   consumer ;
        p<IMessageProducer>   producer ;
        p<ITextMessage>       reqMessage,
                              rspMessage ;
        p<Uri>                uri ;
        p<PropertyMap>        props ;

        cout << "Connecting to ActiveMQ broker..." << endl ;

        uri = new Uri("tcp://127.0.0.1:61616?trace=true&protocol=openwire") ;
        factory = new ConnectionFactory(uri) ;
        connection = factory->createConnection() ;

        // Create session
        session = connection->createSession() ;

        // Connect to queue
        queue = session->getQueue("FOO.BAR") ;

        cout << "Using destination: " << queue->getQueueName()->c_str() << endl ;

        // Create a consumer and producer
        consumer = session->createConsumer(queue) ;
        producer = session->createProducer(queue) ;
        producer->setPersistent(true) ;

        // Create a message
        reqMessage = session->createTextMessage("Hello World!") ;
        reqMessage->setJMSCorrelationID("abc") ;
        reqMessage->setJMSXGroupID("cheese") ;
        props = reqMessage->getProperties() ;
        (*props)["someHeader"] = MapItemHolder( "James" ) ;

        // Send message
        producer->send(reqMessage) ;

        cout << "Waiting for asynchrounous receive message..." << endl ;

        // Receive and wait for a message
        rspMessage = p_dyncast<ActiveMQTextMessage> (consumer->receive()) ;
        if( rspMessage == NULL )
            cout << "No message received!" << endl ;
        else
        {
            cout << "Received message with ID: " << rspMessage->getJMSMessageID()->c_str() << endl ;
            cout << "                and text: " << rspMessage->getText()->c_str() << endl ;
        }
        // Shutdown gracefully (including all attached sub-items, sessions, consumer/producer)
        connection->close() ;

        cout << "Disconnected from ActiveMQ broker" << endl ;
    }
    catch( TraceException& te )
    {
        cout << "Caught: " << te.what() << endl ;
        //cout << "Stack: " << e.getStackTrace() ;
    }
    catch( exception& e )
    {
        cout << "Caught: " << e.what() << endl ;
        //cout << "Stack: " << e.getStackTrace() ;
    }
}

/*
 * Tests asynchronous sending/receiving of a binary message
 */
void testAsyncByteMessage()
{
    try
    {
        p<IConnectionFactory> factory ;
        p<IConnection>        connection ;
        p<ISession>           session ;
        p<IQueue>             queue ;
        p<IMessageConsumer>   consumer ;
        p<IMessageProducer>   producer ;
        p<IBytesMessage>      reqMessage,
                              rspMessage ;
        p<Uri>                uri ;
        p<PropertyMap>        props ;
        p<TestListener>       listener ;

        cout << "Connecting to ActiveMQ broker..." << endl ;

        uri = new Uri("tcp://127.0.0.1:61616?trace=true&protocol=openwire") ;
        factory = new ConnectionFactory(uri) ;
        connection = factory->createConnection() ;

        // Create session
        session = connection->createSession() ;

        // Connect to queue
        queue = session->getQueue("FOO.BAR") ;

        cout << "Using destination: " << queue->getQueueName()->c_str() << endl ;

        // Create producer and a asycnhrounous consumer
        producer = session->createProducer(queue) ;
        producer->setPersistent(true) ;
        consumer = session->createConsumer(queue) ;
        listener = new TestListener() ;
        consumer->setMessageListener(listener) ;

        // Create binary message
        reqMessage = session->createBytesMessage() ;
        reqMessage->writeBoolean(true) ;
        reqMessage->writeInt(3677490) ;
        reqMessage->writeUTF("Hello Binary World!") ;

        // Send message
        producer->send(reqMessage) ;

        // Wait for asynchronous message
        char c = getchar() ;

        // Shutdown gracefully (including all attached sub-items, sessions, consumer/producer)
        connection->close() ;

        cout << "Disconnected from ActiveMQ broker" << endl ;
    }
    catch( TraceException& te )
    {
        cout << "Caught: " << te.what() << endl ;
        //cout << "Stack: " << e.getStackTrace() ;
    }
    catch( exception& e )
    {
        cout << "Caught: " << e.what() << endl ;
        //cout << "Stack: " << e.getStackTrace() ;
    }
}

int main()
{
    testSyncTextMessage() ;
    testAsyncByteMessage() ;
}
