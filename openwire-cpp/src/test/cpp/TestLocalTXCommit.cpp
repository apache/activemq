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
#include "TestLocalTXCommit.hpp"

/*
 * 
 */
TestLocalTXCommit::TestLocalTXCommit(p<IConnection> connection)
{
    this->connection  = connection ;
    this->error       = NULL ;
    this->transmitted = false ;
    this->matchCount  = 0 ;
}

/*
 * 
 */
TestLocalTXCommit::~TestLocalTXCommit()
{
    // no-op
}

/*
 * 
 */
void TestLocalTXCommit::setUp() throw (exception)
{
    // Create a session
    session = connection->createSession( TransactionalAckMode ) ;
}

/*
 * 
 */
void TestLocalTXCommit::execute() throw (exception)
{
    p<IQueue>             queue ;
    p<IMessageConsumer>   consumer ;
    p<IMessageProducer>   producer ;
    p<ITextMessage>       message1, message2;

    // Connect to a queue
    queue = session->getQueue("FOO.BAR") ;

    // Create producer and a asycnhrounous consumer
    producer = session->createProducer(queue) ;
    consumer = session->createConsumer(queue) ;
    consumer->setMessageListener( smartify(this) ) ;

    // Create text messages
    message1 = session->createTextMessage("LocalTX 1") ;
    message2 = session->createTextMessage("LocalTX 2") ;

    // Send messages
    producer->send(message1) ;
    producer->send(message2) ;

    // Commit transaction
    transmitted = true ;
    session->commit() ;

    // Wait for asynchronous receive for 5s
    semaphore.wait(5) ;

    // Check if any error was registered by the message handler
    if( error != NULL )
        throw TraceException( error ) ;
}

/*
 * 
 */
void TestLocalTXCommit::tearDown() throw (exception)
{
    // Clean up
    session->close() ;
    session = NULL ;
}

p<string> TestLocalTXCommit::toString()
{
    p<string> str = new string("Sends multiple messages to a queue guarded by a local transaction") ;
    return str ;
}

void TestLocalTXCommit::onMessage(p<IMessage> message)
{
    if( !transmitted )
    {
        error = "Received a message before transaction was committed" ;
        session->rollback() ;
        semaphore.notify() ;
        return ;
    }

    if( message == NULL )
    {
        error = "Received a null message" ;
        session->rollback() ;
        semaphore.notify() ;
        return ;
    }

    p<ITextMessage> msg = p_dyncast<ITextMessage> (message) ;
    if( msg == NULL )
    {
        error = "Received wrong type of message" ;
        semaphore.notify() ;
        return ;
    }

    // Verify message content
    if( msg->getText()->compare("LocalTX 1") != 0 ||
        msg->getText()->compare("LocalTX 2") != 0 ) 
    {
        matchCount++ ;
    }
    else
    {
        error = "Message content has been corrupted" ;

        // Rollback receive
        session->rollback() ;

        // Wakeup main thread
        semaphore.notify() ;
    }

    // Did we receive both messages?
    if( matchCount == 2 )
    {
        // Commit receive
        session->commit() ;

        // Wakeup main thread
        semaphore.notify() ;
    }
}
