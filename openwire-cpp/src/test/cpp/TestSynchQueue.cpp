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
#include "TestSynchQueue.hpp"

/*
 * 
 */
TestSynchQueue::TestSynchQueue(p<IConnection> connection)
{
    this->connection = connection ;
}

/*
 * 
 */
TestSynchQueue::~TestSynchQueue()
{
    // no-op
}

/*
 * 
 */
void TestSynchQueue::setUp() throw (exception)
{
    // Create a session
    session = connection->createSession() ;
}

/*
 * 
 */
void TestSynchQueue::execute() throw (exception)
{
    p<IQueue>             queue ;
    p<IMessageConsumer>   consumer ;
    p<IMessageProducer>   producer ;
    p<ITextMessage>       reqMessage,
                          rspMessage ;
    p<PropertyMap>        props ;
    MapItemHolder         item ;

    // Connect to queue
    queue = session->getQueue("FOO.BAR") ;
    
    // Create a consumer and producer
    consumer = session->createConsumer(queue) ;
    producer = session->createProducer(queue) ;

    // Create a message
    reqMessage = session->createTextMessage("Hello World!") ;
    reqMessage->setJMSCorrelationID("abc") ;
    reqMessage->setJMSXGroupID("cheese") ;
    props = reqMessage->getProperties() ;
    (*props)["someHeader"] = MapItemHolder( "James" ) ;

    // Send message
    producer->send(reqMessage) ;

    // Receive and wait for a message
    rspMessage = p_dyncast<ITextMessage> (consumer->receive()) ;
    if( rspMessage == NULL )
        throw TraceException("Received a null message") ;
    else
    {
        p<string> str ;

        props = rspMessage->getProperties() ;
        item  = (*props)["someHeader"] ;

        // Verify message
        str = rspMessage->getJMSCorrelationID() ;
        if( str == NULL || str->compare("abc") != 0 )
            throw TraceException("Returned message has invalid correlation ID") ;

        str = rspMessage->getJMSXGroupID() ;
        if( str == NULL || str->compare("cheese") != 0 )
            throw TraceException("Returned message has invalid group ID") ;

        str = rspMessage->getText() ;
        if( str == NULL || str->compare("Hello World!") != 0 )
            throw TraceException("Returned message has altered body text") ;

        str = item.getString() ;
        if( str == NULL || str->compare("James") != 0 )
            throw TraceException("Returned message has invalid properties") ;
    }
}

/*
 * 
 */
void TestSynchQueue::tearDown() throw (exception)
{
    // Clean up
    session->close() ;
    session = NULL ;
}

p<string> TestSynchQueue::toString()
{
    p<string> str = new string("Send/receive a text message to a queue synchronously") ;
    return str ;
}
