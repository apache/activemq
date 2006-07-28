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
#include "TestAsynchTopic.hpp"

/*
 * 
 */
TestAsynchTopic::TestAsynchTopic(p<IConnection> connection)
{
    this->connection = connection ;
    this->error      = NULL ;
}

/*
 * 
 */
TestAsynchTopic::~TestAsynchTopic()
{
    // no-op
}

/*
 * 
 */
void TestAsynchTopic::setUp() throw (exception)
{
    // Create a session
    session = connection->createSession() ;
}

/*
 * 
 */
void TestAsynchTopic::execute() throw (exception)
{
    p<ITopic>           topic ;
    p<IMessageConsumer> consumer1,
                        consumer2 ;
    p<IMessageProducer> producer ;
    p<IMapMessage>      message ;

    // Connect to a topic
    topic = session->getTopic("TEST.TOPIC") ;

    // Create producer and two asynchrounous consumers
    producer = session->createProducer(topic) ;
    consumer1 = session->createConsumer(topic) ;
    consumer1->setMessageListener( smartify(this) ) ;
    consumer2 = session->createConsumer(topic) ;
    consumer2->setMessageListener( smartify(this) ) ;

    // Create binary message
    message = session->createMapMessage() ;
    message->setBoolean("key1", false) ;
    message->setInt("key2", 8494845) ;
    message->setString("key3", "Hello Map World!") ;

    // Send message
    producer->send(message) ;

    // Wait for asynchronous messages for 5s each
    semaphore.wait(5) ;
    semaphore.wait(5) ;

    // Check if any error was registered by the message handlers
    if( error != NULL )
        throw TraceException( error ) ;
}

/*
 * 
 */
void TestAsynchTopic::tearDown() throw (exception)
{
    // Clean up
    session->close() ;
    session = NULL ;
}

p<string> TestAsynchTopic::toString()
{
    p<string> str = new string("Send/receive a map message to 2 topic listener asynchronously") ;
    return str ;
}

void TestAsynchTopic::onMessage(p<IMessage> message)
{
    if( message == NULL )
    {
        error = "Received a null message" ;
        semaphore.notify() ;
        return ;
    }

    p<IMapMessage> msg = p_dyncast<IMapMessage> (message) ;
    if( msg == NULL )
    {
        error = "Received wrong type of message" ;
        semaphore.notify() ;
        return ;
    }

    // Verify message content
    if( msg->getBoolean("key1") != false ||
        msg->getInt("key2")     != 8494845 ||
        msg->getString("key3")->compare("Hello Map World!") != 0 ) 
    {
        error = "Message content has been corrupted" ;
    }
    semaphore.notify() ;
}
