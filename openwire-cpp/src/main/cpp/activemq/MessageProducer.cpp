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
#include "activemq/MessageProducer.hpp"
#include "activemq/Session.hpp"

using namespace apache::activemq;


// Constructors -----------------------------------------------------

/*
 * 
 */
MessageProducer::MessageProducer(p<Session> session, p<ProducerInfo> producerInfo)
{
    this->session                 = session ;
    this->producerInfo            = producerInfo ;
    this->priority                = DEFAULT_PRIORITY ;
    this->timeToLive              = DEFAULT_TIMETOLIVE ;
    this->messageCounter          = 0 ;
    this->persistent              = false ;
    this->disableMessageID        = false ;
    this->disableMessageTimestamp = false ;
    this->closed                  = false ;
}

/*
 * 
 */
MessageProducer::~MessageProducer()
{
    // Make sure the producer is closed
    close() ;
}


// Attribute methods ------------------------------------------------

bool MessageProducer::getPersistent()
{
    return persistent ;
}

void MessageProducer::setPersistent(bool persistent)
{
    this->persistent = persistent ;
}

long long MessageProducer::getTimeToLive()
{
    return timeToLive ;
}

void MessageProducer::getTimeToLive(long long ttl)
{
    this->timeToLive = ttl ;
}

int MessageProducer::getPriority()
{
    return priority ;
}

void MessageProducer::getPriority(int priority)
{
    this->priority = priority ;
}

bool MessageProducer::getDisableMessageID()
{
    return disableMessageID ;
}

void MessageProducer::getDisableMessageID(bool disable)
{
    this->disableMessageID = disable ;
}

bool MessageProducer::getDisableMessageTimestamp()
{
    return disableMessageTimestamp ;
}

void MessageProducer::getDisableMessageTimestamp(bool disable)
{
    this->disableMessageTimestamp = disable ;
}


// Operation methods ------------------------------------------------

/*
 * 
 */
void MessageProducer::send(p<IMessage> message)
{
    send(producerInfo->getDestination(), message, DEFAULT_PRIORITY, DEFAULT_TIMETOLIVE) ;
}

/*
 * 
 */
void MessageProducer::send(p<IDestination> destination, p<IMessage> message)
{
    send(destination, message, DEFAULT_PRIORITY, DEFAULT_TIMETOLIVE) ;
}

/*
 * 
 */
void MessageProducer::send(p<IDestination> destination, p<IMessage> message, char priority, long long timeToLive)
{
    p<MessageId> msgId = new MessageId() ;
    msgId->setProducerId( producerInfo->getProducerId() ) ;

    // Acquire next sequence id
    {
        LOCKED_SCOPE (mutex);
        msgId->setProducerSequenceId( ++messageCounter ) ;
    }

    // Configure the message
    p<ActiveMQMessage> activeMessage = p_dyncast<ActiveMQMessage> (message) ;
    activeMessage->setMessageId( msgId ) ;
    activeMessage->setProducerId( producerInfo->getProducerId() ) ;
    activeMessage->setDestination( p_dyncast<ActiveMQDestination> (destination) ) ;
    activeMessage->setPriority(priority) ;

    if( session->isTransacted() )
    {
        session->doStartTransaction() ;
        activeMessage->setTransactionId( session->getTransactionContext()->getTransactionId() ) ;
    }

    // Set time values if not disabled
    if( !this->disableMessageTimestamp )
    {
        long long timestamp = Time::getCurrentTimeMillis() ;

        // Set message time stamp/expiration
        activeMessage->setTimestamp(timestamp) ;
        if( timeToLive > 0 )
            activeMessage->setExpiration( timestamp + timeToLive ) ;
    }

    // Finally, transmit the message
    session->doSend(destination, message) ;
}

/*
 * 
 */
void MessageProducer::close()
{
    if( !closed )
    {
        closed = true ;
    
        // De-register producer from broker
        session->getConnection()->disposeOf( producerInfo->getProducerId() ) ;

        // Reset internal state (prevent cyclic references)
        session = NULL ;
    }
}
