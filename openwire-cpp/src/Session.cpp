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
#include "Session.hpp"
#include "command/ActiveMQQueue.hpp"
#include "command/ActiveMQTopic.hpp"
#include "command/ActiveMQMessage.hpp"
#include "command/ActiveMQTextMessage.hpp"
#include "command/ProducerInfo.hpp"
#include "command/ConsumerInfo.hpp"
#include "command/MessageAck.hpp"
#include "MessageConsumer.hpp"
#include "MessageProducer.hpp"
#include "Connection.hpp"

using namespace apache::activemq::client;

/*
 * 
 */
Session::Session(p<Connection> connection, p<SessionInfo> info)
{
    this->connection  = connection ;
    this->sessionInfo = info ;
}

Session::~Session()
{
    // De-activate session
    disposeOf(sessionInfo->getSessionId()) ;  //(IDataStructure*)
}

p<IMessageProducer> Session::createProducer()
{
    return createProducer(NULL) ; 
}

p<IMessageProducer> Session::createProducer(p<IDestination> destination)
{
    p<ProducerInfo> command = createProducerInfo(destination) ;
    connection->syncRequest(command) ;  //(ICommand*)
    p<IMessageProducer> messageProducer = new MessageProducer(this, command) ;
    return messageProducer ;
}

void Session::acknowledge(p<IMessage> message)
{
    if( ackMode == ClientMode )
    {
        p<MessageAck> msgAck = new MessageAck() ;
        // TODO complete packet
        connection->syncRequest(msgAck) ;  //(ICommand*)
    } 
}

p<IMessageConsumer> Session::createConsumer(p<IDestination> destination)
{
    return createConsumer(destination, NULL) ; 
}

p<IMessageConsumer> Session::createConsumer(p<IDestination> destination, const char* selector)
{
    p<ConsumerInfo> command = createConsumerInfo(destination, selector) ;
    connection->syncRequest(command) ;  //(ICommand*)
    p<IMessageConsumer> messageConsumer = new MessageConsumer(this, command) ; 
    return messageConsumer ;
}

p<IQueue> Session::getQueue(const char* name)
{
    p<IQueue> queue = (IQueue*)new ActiveMQQueue(name) ;
    return queue ;
}

p<ITopic> Session::getTopic(const char* name)
{
    p<ITopic> topic = (ITopic*)new ActiveMQTopic(name) ;
    return topic ;
}

p<IMessage> Session::createMessage()
{
    p<IMessage> message = (IMessage*)new ActiveMQMessage() ;
    return message ;
}

p<ITextMessage> Session::createTextMessage()
{
    p<ITextMessage> message = (ITextMessage*)new ActiveMQTextMessage() ;
    return message ;
}

p<ITextMessage> Session::createTextMessage(const char* text)
{
    p<ITextMessage> message = (ITextMessage*)new ActiveMQTextMessage(text) ;
    return message ;
}

void Session::doSend(p<IDestination> destination, p<IMessage> message)
{
    p<ActiveMQMessage> command = ActiveMQMessage::transform(message) ;
    // TODO complete packet
    connection->syncRequest(command) ; //(ICommand*)
}

void Session::disposeOf(p<IDataStructure> objectId)
{
    p<RemoveInfo> command = new RemoveInfo() ;
    command->setObjectId( objectId ) ;

    connection->syncRequest(command) ;  //(ICommand*)
}

// Implementation methods ------------------------------------------

p<ConsumerInfo> Session::createConsumerInfo(p<IDestination> destination, const char* selector)
{
    p<ConsumerInfo> consumerInfo = new ConsumerInfo() ;
    p<ConsumerId> consumerId = new ConsumerId() ;
    consumerId->setSessionId( sessionInfo->getSessionId()->getValue() ) ;

    mutex.lock() ;
    consumerId->setValue( ++consumerCounter ) ;
    mutex.unlock() ;

    // TODO complete packet
    consumerInfo->setConsumerId( consumerId ) ;

    return consumerInfo ;
}

p<ProducerInfo> Session::createProducerInfo(p<IDestination> destination)
{
    p<ProducerInfo> producerInfo = new ProducerInfo() ;
    // TODO complete packet
    return producerInfo ;
} 
