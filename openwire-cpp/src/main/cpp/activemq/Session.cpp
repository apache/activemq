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
#include "activemq/Session.hpp"
#include "activemq/command/ActiveMQDestination.hpp"
#include "activemq/command/ActiveMQQueue.hpp"
#include "activemq/command/ActiveMQTopic.hpp"
#include "activemq/command/ActiveMQTempQueue.hpp"
#include "activemq/command/ActiveMQTempTopic.hpp"
#include "activemq/command/ActiveMQMessage.hpp"
#include "activemq/command/ActiveMQBytesMessage.hpp"
#include "activemq/command/ActiveMQMapMessage.hpp"
#include "activemq/command/ActiveMQTextMessage.hpp"
#include "activemq/command/ProducerInfo.hpp"
#include "activemq/command/ConsumerInfo.hpp"
#include "activemq/command/MessageAck.hpp"
#include "activemq/MessageConsumer.hpp"
#include "activemq/MessageProducer.hpp"
#include "activemq/Connection.hpp"

using namespace apache::activemq;


// Constructors -----------------------------------------------------

/*
 * 
 */
Session::Session(p<Connection> connection, p<SessionInfo> info, AcknowledgementMode ackMode)
{
    this->connection         = connection ;
    this->sessionInfo        = info ;
    this->ackMode            = ackMode ;
    this->prefetchSize       = 1000 ;
    this->consumerCounter    = 0 ;
    this->producerCounter    = 0 ;
    this->transactionContext = new TransactionContext(smartify(this)) ;
    this->dispatchThread     = new DispatchThread(smartify(this)) ;
    this->closed             = false ;

    // Activate backround dispatch thread
    dispatchThread->start() ;
}

/*
 * 
 */
Session::~Session()
{
    // Make sure session is closed
    close() ;
}


// Attribute methods ------------------------------------------------

/*
 * 
 */
bool Session::isTransacted()
{
    return ( ackMode == TransactionalAckMode ) ? true : false ; 
}

/*
 * 
 */
p<Connection> Session::getConnection()
{
    return connection ;
}

/*
 * 
 */
p<SessionId> Session::getSessionId()
{
    return sessionInfo->getSessionId() ;
}

/*
 * 
 */
p<TransactionContext> Session::getTransactionContext()
{
    return transactionContext ;
}

/*
 * 
 */
p<MessageConsumer> Session::getConsumer(p<ConsumerId> consumerId)
{
    map<long long, p<MessageConsumer> >::iterator tempIter ;

    // Check if key exists in map
    tempIter = consumers.find( consumerId->getValue() ) ;
    if( tempIter == consumers.end() )
        return NULL ;
    else
        return tempIter->second ;
}

/*
 * 
 */
p<MessageProducer> Session::getProducer(p<ProducerId> producerId)
{
    map<long long, p<MessageProducer> >::iterator tempIter ;

    // Check if key exists in map
    tempIter = producers.find( producerId->getValue() ) ;
    if( tempIter == producers.end() )
        return NULL ;
    else
        return tempIter->second ;
}


// Operation methods ------------------------------------------------

/*
 * 
 */
p<IMessageProducer> Session::createProducer()
{
    return createProducer(NULL) ; 
}

/*
 * 
 */
p<IMessageProducer> Session::createProducer(p<IDestination> destination)
{
    p<ProducerInfo> command  = createProducerInfo(destination) ;
    p<ProducerId> producerId = command->getProducerId() ;

    try
    {
        p<MessageProducer> producer = new MessageProducer(smartify(this), command) ;

        // Save the producer
        producers[ producerId->getValue() ] = producer ;

        // Register producer with broker
        connection->syncRequest(command) ;

        return producer ;
    }
    catch( exception e )
    {
        // Make sure producer was removed
        producers[ producerId->getValue() ] = NULL ;
        throw e ;
    }
}

/*
 * 
 */
p<IMessageConsumer> Session::createConsumer(p<IDestination> destination)
{
    return createConsumer(destination, NULL) ; 
}

/*
 * 
 */
p<IMessageConsumer> Session::createConsumer(p<IDestination> destination, const char* selector)
{
    p<ConsumerInfo> command  = createConsumerInfo(destination, selector) ;
    p<ConsumerId> consumerId = command->getConsumerId() ;

    try
    {
        p<MessageConsumer> consumer = new MessageConsumer(smartify(this), command, ackMode) ;

        // Save the consumer first in case message dispatching starts immediately
        consumers[ consumerId->getValue() ] = consumer ;

        // Register consumer with broker
        connection->syncRequest(command) ;

        return consumer ;
    }
    catch( exception e )
    {
        // Make sure consumer was removed
        consumers[ consumerId->getValue() ] = NULL ;
        throw e ;
    }
}

p<IMessageConsumer> Session::createDurableConsumer(p<ITopic> destination, const char* name, const char* selector, bool noLocal)
{
    p<ConsumerInfo> command  = createConsumerInfo(destination, selector) ;
    p<ConsumerId> consumerId = command->getConsumerId() ;
    p<string>     subscriptionName = new string(name) ;

    command->setSubcriptionName( subscriptionName ) ;
    command->setNoLocal( noLocal ) ;
    
    try
    {
        p<MessageConsumer> consumer = new MessageConsumer(smartify(this), command, ackMode) ;

        // Save the consumer first in case message dispatching starts immediately
        consumers[ consumerId->getValue() ] = consumer ;

        // Register consumer with broker
        connection->syncRequest(command) ;

        return consumer ;
    }
    catch( exception e )
    {
        // Make sure consumer was removed
        consumers[ consumerId->getValue() ] = NULL ;
        throw e ;
    }
}

/*
 * 
 */
p<IQueue> Session::getQueue(const char* name)
{
    p<IQueue> queue = new ActiveMQQueue(name) ;
    return queue ;
}

/*
 * 
 */
p<ITopic> Session::getTopic(const char* name)
{
    p<ITopic> topic = new ActiveMQTopic(name) ;
    return topic ;
}

/*
 * 
 */
p<ITemporaryQueue> Session::createTemporaryQueue()
{
    p<ITemporaryQueue> queue = new ActiveMQTempQueue( connection->createTemporaryDestinationName()->c_str() ) ;
    return queue ;
}

/*
 * 
 */
p<ITemporaryTopic> Session::createTemporaryTopic()
{
    p<ITemporaryTopic> topic = new ActiveMQTempTopic( connection->createTemporaryDestinationName()->c_str() ) ;
    return topic ;
}

/*
 * 
 */
p<IMessage> Session::createMessage()
{
    p<IMessage> message = new ActiveMQMessage() ;
    configure(message) ;
    return message ;
}

/*
 * 
 */
p<IBytesMessage> Session::createBytesMessage()
{
    p<IBytesMessage> message = new ActiveMQBytesMessage() ;
    configure(message) ;
    return message ;
}

/*
 * 
 */
p<IBytesMessage> Session::createBytesMessage(char* body, int size)
{
    p<IBytesMessage> message = new ActiveMQBytesMessage( body, size ) ;
    configure(message) ;
    return message ;
}

/*
 * 
 */
p<IMapMessage> Session::createMapMessage()
{
    p<IMapMessage> message = new ActiveMQMapMessage() ;
    configure(message) ;
    return message ;
}

/*
 * 
 */
p<ITextMessage> Session::createTextMessage()
{
    p<ITextMessage> message = new ActiveMQTextMessage() ;
    configure(message) ;
    return message ;
}

/*
 * 
 */
p<ITextMessage> Session::createTextMessage(const char* text)
{
    p<ITextMessage> message = new ActiveMQTextMessage(text) ;
    configure(message) ;
    return message ;
}

/*
 * 
 */
void Session::commit() throw(CmsException)
{
    if( !isTransacted() )
        throw CmsException("You cannot perform a commit on a non-transacted session. Acknowlegement mode is: " + ackMode) ;

    transactionContext->commit() ;
}

/*
 * 
 */
void Session::rollback() throw(CmsException)
{
    if( !isTransacted() )
        throw CmsException("You cannot perform a rollback on a non-transacted session. Acknowlegement mode is: " + ackMode) ;

    transactionContext->rollback() ;

    map<long long, p<MessageConsumer> >::const_iterator tempIter ;

    // Ensure all the consumers redeliver any rolled back messages
    for( tempIter = consumers.begin() ;
         tempIter != consumers.end() ;
         tempIter++ )
    {
        ((*tempIter).second)->redeliverRolledBackMessages() ;
    }
}

/*
 * 
 */
void Session::doSend(p<IDestination> destination, p<IMessage> message)
{
    p<ActiveMQMessage> command = p_dyncast<ActiveMQMessage> (message) ;
    // TODO complete packet
    connection->syncRequest(command) ;
}

/*
 * Starts a new transaction
 */
void Session::doStartTransaction()
{
    if( isTransacted() )
        transactionContext->begin() ;
}

/*
 * 
 */
void Session::dispatch(int delay)
{
    if( delay > 0 ) 
        dispatchThread->sleep(delay) ;

    dispatchThread->wakeup() ;
}

/*
 * 
 */
void Session::dispatchAsyncMessages()
{
    // Ensure that only 1 thread dispatches messages in a consumer at once
    LOCKED_SCOPE (mutex);

    map<long long, p<MessageConsumer> >::const_iterator tempIter ;

    // Iterate through each consumer created by this session
    // ensuring that they have all pending messages dispatched
    for( tempIter = consumers.begin() ;
         tempIter != consumers.end() ;
         tempIter++ )
    {
        ((*tempIter).second)->dispatchAsyncMessages() ;
    }
}

/*
 *
 */
void Session::close()
{
    if( !closed )
    {
        map<long long, p<MessageConsumer> >::iterator consumerIter ;
        map<long long, p<MessageProducer> >::iterator producerIter ;

        // Shutdown dispatch thread
        dispatchThread->interrupt() ;
        dispatchThread->join() ;
        dispatchThread = NULL ;

        // Iterate through all consumers and close them down
        for( consumerIter = consumers.begin() ;
             consumerIter != consumers.end() ;
             consumerIter++ )
        {
            consumerIter->second->close() ;
            consumerIter->second = NULL ;
        }

        // Iterate through all producers and close them down
        for( producerIter = producers.begin() ;
             producerIter != producers.end() ;
             producerIter++ )
        {
            producerIter->second->close() ;
            producerIter->second = NULL ;
        }
        // De-register session from broker/connection
        connection->disposeOf( sessionInfo->getSessionId() ) ;

        // Clean up
        connection = NULL ;
        closed     = true ;

    }
}


// Implementation methods ------------------------------------------

/*
 * 
 */
p<ConsumerInfo> Session::createConsumerInfo(p<IDestination> destination, const char* selector)
{
    p<ConsumerInfo> consumerInfo = new ConsumerInfo() ;
    p<ConsumerId> consumerId = new ConsumerId() ;

    consumerId->setConnectionId( sessionInfo->getSessionId()->getConnectionId() ) ;
    consumerId->setSessionId( sessionInfo->getSessionId()->getValue() ) ;

    {
        LOCKED_SCOPE (mutex);
        consumerId->setValue( ++consumerCounter ) ;
    }
    p<string> sel = ( selector == NULL ) ? NULL : new string(selector) ;

    // TODO complete packet
    consumerInfo->setConsumerId( consumerId ) ;
    consumerInfo->setDestination( p_dyncast<ActiveMQDestination> (destination) ) ; //ActiveMQDestination::transform(destination) ) ;
    consumerInfo->setSelector( sel ) ;
    consumerInfo->setPrefetchSize( this->prefetchSize ) ;

    return consumerInfo ;
}

/*
 * 
 */
p<ProducerInfo> Session::createProducerInfo(p<IDestination> destination)
{
    p<ProducerInfo> producerInfo = new ProducerInfo() ;
    p<ProducerId> producerId = new ProducerId() ;

    producerId->setConnectionId( sessionInfo->getSessionId()->getConnectionId() ) ;
    producerId->setSessionId( sessionInfo->getSessionId()->getValue() ) ;

    {
        LOCKED_SCOPE (mutex);
        producerId->setValue( ++producerCounter ) ;
    }

    // TODO complete packet
    producerInfo->setProducerId( producerId ) ;
    producerInfo->setDestination( p_dyncast<ActiveMQDestination> (destination) ) ; //ActiveMQDestination::transform(destination) ) ;

    return producerInfo ;
} 

/*
 * Configures the message command.
 */
void Session::configure(p<IMessage> message)
{
    // TODO:
}
