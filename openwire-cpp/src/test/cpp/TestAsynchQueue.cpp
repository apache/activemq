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
#include "TestAsynchQueue.hpp"

/*
 * 
 */
TestAsynchQueue::TestAsynchQueue(p<IConnection> connection)
{
    this->connection = connection ;
    this->error      = NULL ;
}

/*
 * 
 */
TestAsynchQueue::~TestAsynchQueue()
{
    // no-op
}

/*
 * 
 */
void TestAsynchQueue::setUp() throw (exception)
{
    // Create a session
    session = connection->createSession() ;
}

/*
 * 
 */
void TestAsynchQueue::execute() throw (exception)
{
    p<IQueue>             queue ;
    p<IMessageConsumer>   consumer ;
    p<IMessageProducer>   producer ;
    p<IBytesMessage>      message ;

    // Connect to a queue
    queue = session->getQueue("FOO.BAR") ;

    // Create producer and a asycnhrounous consumer
    producer = session->createProducer(queue) ;
    consumer = session->createConsumer(queue) ;
    consumer->setMessageListener( smartify(this) ) ;

    // Create binary message
    message = session->createBytesMessage() ;
    message->writeBoolean(true) ;
    message->writeInt(3677490) ;
    message->writeString("Hello Binary World!") ;

    // Send message
    producer->send(message) ;

    // Wait for asynchronous message for 5s
    semaphore.wait(5) ;

    // Check if any error was registered by the message handler
    if( error != NULL )
        throw TraceException( error ) ;
}

/*
 * 
 */
void TestAsynchQueue::tearDown() throw (exception)
{
    // Clean up
    session->close() ;
    session = NULL ;
}

p<string> TestAsynchQueue::toString()
{
    p<string> str = new string("Send/receive a byte message to 1 queue listener asynchronously") ;
    return str ;
}

void TestAsynchQueue::onMessage(p<IMessage> message)
{
    if( message == NULL )
    {
        error = "Received a null message" ;
        semaphore.notify() ;
        return ;
    }

    p<IBytesMessage> msg = p_dyncast<IBytesMessage> (message) ;
    if( msg == NULL )
    {
        error = "Received wrong type of message" ;
        semaphore.notify() ;
        return ;
    }

    // Verify message content
    if( msg->readBoolean() != true ||
        msg->readInt()     != 3677490 ||
        msg->readString()->compare("Hello Binary World!") != 0 ) 
    {
        error = "Message content has been corrupted" ;
    }
    semaphore.notify() ;
}
