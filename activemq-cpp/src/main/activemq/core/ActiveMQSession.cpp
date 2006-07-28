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
#include "ActiveMQSession.h"

#include <activemq/exceptions/InvalidStateException.h>
#include <activemq/exceptions/NullPointerException.h>

#include <activemq/core/ActiveMQConnection.h>
#include <activemq/core/ActiveMQTransaction.h>
#include <activemq/core/ActiveMQConsumer.h>
#include <activemq/core/ActiveMQMessage.h>
#include <activemq/core/ActiveMQProducer.h>

#include <activemq/connector/TransactionInfo.h>

using namespace std;
using namespace cms;
using namespace activemq;
using namespace activemq::core;
using namespace activemq::util;
using namespace activemq::connector;
using namespace activemq::exceptions;

////////////////////////////////////////////////////////////////////////////////
ActiveMQSession::ActiveMQSession( SessionInfo* sessionInfo,
                                  const Properties& properties,
                                  ActiveMQConnection* connection)
{
    if( sessionInfo == NULL || connection == NULL )
    {
        throw NullPointerException(
            __FILE__, __LINE__,
            "ActiveMQSession::ActiveMQSession - Init with NULL data");
    }

    this->sessionInfo = sessionInfo;
    this->transaction = NULL;
    this->connection  = connection;
    this->closed      = false;

    // Create a Transaction object only if the session is transactional
    if( isTransacted() )
    {
        transaction = 
            new ActiveMQTransaction(connection, this, properties );
    }
}

////////////////////////////////////////////////////////////////////////////////
ActiveMQSession::~ActiveMQSession(void)
{
    try
    {
        // Destroy this session's resources
        close();
    }
    AMQ_CATCH_NOTHROW( ActiveMQException )
    AMQ_CATCHALL_NOTHROW( )
}

////////////////////////////////////////////////////////////////////////////////
void ActiveMQSession::close(void) throw ( cms::CMSException )
{
    if(closed)
    {
        return;
    }

    try
    {
        // Destry the Transaction
        delete transaction;

        // Destroy this sessions resources
        connection->getConnectionData()->
            getConnector()->destroyResource( sessionInfo );

        // mark as done
        closed = true;
    }
    AMQ_CATCH_NOTHROW( ActiveMQException )
    AMQ_CATCHALL_NOTHROW( )
}

////////////////////////////////////////////////////////////////////////////////
void ActiveMQSession::commit(void) throw ( cms::CMSException )
{
    try
    {
        if( closed || !isTransacted() )
        {
            throw InvalidStateException(
                __FILE__, __LINE__,
                "ActiveMQSession::commit - This Session Can't Commit");
        }

        // Commit the Transaction
        transaction->commit();
    }
    AMQ_CATCH_NOTHROW( ActiveMQException )
    AMQ_CATCHALL_NOTHROW( )
}

////////////////////////////////////////////////////////////////////////////////
void ActiveMQSession::rollback(void) throw ( cms::CMSException )
{
    try
    {
        if( closed || !isTransacted() )
        {
            throw InvalidStateException(
                __FILE__, __LINE__,
                "ActiveMQSession::rollback - This Session Can't Rollback" );
        }

        // Rollback the Transaction
        transaction->rollback();
    }
    AMQ_CATCH_NOTHROW( ActiveMQException )
    AMQ_CATCHALL_NOTHROW( )
}

////////////////////////////////////////////////////////////////////////////////
cms::MessageConsumer* ActiveMQSession::createConsumer(
    const cms::Destination* destination )
        throw ( cms::CMSException )
{
    try
    {
        if( closed )
        {
            throw InvalidStateException(
                __FILE__, __LINE__,
                "ActiveMQSession::createConsumer - Session Already Closed" );
        }

        return createConsumer( destination, "" );
    }
    AMQ_CATCH_RETHROW( ActiveMQException )
    AMQ_CATCHALL_THROW( ActiveMQException )
}

////////////////////////////////////////////////////////////////////////////////
cms::MessageConsumer* ActiveMQSession::createConsumer(
    const cms::Destination* destination,
    const std::string& selector )
        throw ( cms::CMSException )
{
    try
    {
        if( closed )
        {
            throw InvalidStateException(
                __FILE__, __LINE__,
                "ActiveMQSession::createConsumer - Session Already Closed" );
        }

        ActiveMQConsumer* consumer = new ActiveMQConsumer(
            connection->getConnectionData()->getConnector()->
                createConsumer( destination, sessionInfo, selector), this );

        connection->addMessageListener(
            consumer->getConsumerInfo()->getConsumerId(), consumer );

        return consumer;
    }
    AMQ_CATCH_RETHROW( ActiveMQException )
    AMQ_CATCHALL_THROW( ActiveMQException )
}

////////////////////////////////////////////////////////////////////////////////
cms::MessageConsumer* ActiveMQSession::createDurableConsumer(
    const cms::Topic* destination,
    const std::string& name,
    const std::string& selector,
    bool noLocal )
        throw ( cms::CMSException )
{
    try
    {
        if( closed )
        {
            throw InvalidStateException(
                __FILE__, __LINE__,
                "ActiveMQSession::createProducer - Session Already Closed" );
        }

        ActiveMQConsumer* consumer = new ActiveMQConsumer(
            connection->getConnectionData()->getConnector()->
                createDurableConsumer( destination, sessionInfo, name, selector, noLocal ), this );

        connection->addMessageListener(
            consumer->getConsumerInfo()->getConsumerId(), consumer );

        return consumer;
    }
    AMQ_CATCH_RETHROW( ActiveMQException )
    AMQ_CATCHALL_THROW( ActiveMQException )
}

////////////////////////////////////////////////////////////////////////////////
cms::MessageProducer* ActiveMQSession::createProducer(
    const cms::Destination* destination )
        throw ( cms::CMSException )
{
    try
    {
        if( closed )
        {
            throw InvalidStateException(
                __FILE__, __LINE__,
                "ActiveMQSession::createProducer - Session Already Closed" );
        }

        return new ActiveMQProducer(
            connection->getConnectionData()->getConnector()->
                createProducer( destination, sessionInfo ), this );
    }
    AMQ_CATCH_RETHROW( ActiveMQException )
    AMQ_CATCHALL_THROW( ActiveMQException )
}

////////////////////////////////////////////////////////////////////////////////
cms::Queue* ActiveMQSession::createQueue( const std::string& queueName )
    throw ( cms::CMSException )
{
    try
    {
        if( closed )
        {
            throw InvalidStateException(
                __FILE__, __LINE__,
                "ActiveMQSession::createQueue - Session Already Closed" );
        }

        return connection->getConnectionData()->
            getConnector()->createQueue( queueName, sessionInfo );
    }
    AMQ_CATCH_RETHROW( ActiveMQException )
    AMQ_CATCHALL_THROW( ActiveMQException )
}

////////////////////////////////////////////////////////////////////////////////
cms::Topic* ActiveMQSession::createTopic( const std::string& topicName )
    throw ( cms::CMSException )
{
    try
    {
        if( closed )
        {
            throw InvalidStateException(
                __FILE__, __LINE__,
                "ActiveMQSession::createTopic - Session Already Closed");
        }

        return connection->getConnectionData()->
            getConnector()->createTopic( topicName, sessionInfo );
    }
    AMQ_CATCH_RETHROW( ActiveMQException )
    AMQ_CATCHALL_THROW( ActiveMQException )
}

////////////////////////////////////////////////////////////////////////////////
cms::TemporaryQueue* ActiveMQSession::createTemporaryQueue(void)
    throw ( cms::CMSException )
{
    try
    {
        if( closed )
        {
            throw InvalidStateException(
                __FILE__, __LINE__,
                "ActiveMQSession::createTemporaryQueue - "
                "Session Already Closed" );
        }

        return connection->getConnectionData()->
            getConnector()->createTemporaryQueue( sessionInfo );
    }
    AMQ_CATCH_RETHROW( ActiveMQException )
    AMQ_CATCHALL_THROW( ActiveMQException )
}

////////////////////////////////////////////////////////////////////////////////
cms::TemporaryTopic* ActiveMQSession::createTemporaryTopic(void)
    throw ( cms::CMSException )
{
    try
    {
        if( closed )
        {
            throw InvalidStateException(
                __FILE__, __LINE__,
                "ActiveMQSession::createTemporaryTopic - "
                "Session Already Closed" );
        }

        return connection->getConnectionData()->
            getConnector()->createTemporaryTopic( sessionInfo );
    }
    AMQ_CATCH_RETHROW( ActiveMQException )
    AMQ_CATCHALL_THROW( ActiveMQException )
}

////////////////////////////////////////////////////////////////////////////////
cms::Message* ActiveMQSession::createMessage(void) 
    throw ( cms::CMSException )
{
    try
    {
        if( closed )
        {
            throw InvalidStateException(
                __FILE__, __LINE__,
                "ActiveMQSession::createMessage - Session Already Closed" );
        }

        return connection->getConnectionData()->
            getConnector()->createMessage( sessionInfo, transaction );
    }
    AMQ_CATCH_RETHROW( ActiveMQException )
    AMQ_CATCHALL_THROW( ActiveMQException )
}

////////////////////////////////////////////////////////////////////////////////
cms::BytesMessage* ActiveMQSession::createBytesMessage(void) 
    throw ( cms::CMSException )
{
    try
    {
        if( closed )
        {
            throw InvalidStateException(
                __FILE__, __LINE__,
                "ActiveMQSession::createBytesMessage - Session Already Closed" );
        }

        return connection->getConnectionData()->
            getConnector()->createBytesMessage( sessionInfo, transaction );
    }
    AMQ_CATCH_RETHROW( ActiveMQException )
    AMQ_CATCHALL_THROW( ActiveMQException )
}

////////////////////////////////////////////////////////////////////////////////
cms::BytesMessage* ActiveMQSession::createBytesMessage(
    const unsigned char* bytes,
    unsigned long bytesSize ) 
        throw ( cms::CMSException )
{
    try
    {
        BytesMessage* msg = createBytesMessage();

        msg->setBodyBytes( bytes, bytesSize );

        return msg;
    }
    AMQ_CATCH_RETHROW( ActiveMQException )
    AMQ_CATCHALL_THROW( ActiveMQException )
}

////////////////////////////////////////////////////////////////////////////////
cms::TextMessage* ActiveMQSession::createTextMessage(void) 
    throw ( cms::CMSException )
{
    try
    {
        if( closed )
        {
            throw InvalidStateException(
                __FILE__, __LINE__,
                "ActiveMQSession::createTextMessage - Session Already Closed" );
        }

        return connection->getConnectionData()->
            getConnector()->createTextMessage( sessionInfo, transaction );
    }
    AMQ_CATCH_RETHROW( ActiveMQException )
    AMQ_CATCHALL_THROW( ActiveMQException )
}

////////////////////////////////////////////////////////////////////////////////
cms::TextMessage* ActiveMQSession::createTextMessage( const std::string& text ) 
    throw ( cms::CMSException )
{
    try
    {
        TextMessage* msg = createTextMessage();

        msg->setText( text.c_str() );

        return msg;
    }
    AMQ_CATCH_RETHROW( ActiveMQException )
    AMQ_CATCHALL_THROW( ActiveMQException )
}

////////////////////////////////////////////////////////////////////////////////
cms::MapMessage* ActiveMQSession::createMapMessage(void) 
    throw ( cms::CMSException )
{
    try
    {
        if( closed )
        {
            throw InvalidStateException(
                __FILE__, __LINE__,
                "ActiveMQSession::createMapMessage - Session Already Closed" );
        }

        return connection->
            getConnectionData()->
                getConnector()->createMapMessage( sessionInfo, transaction );
    }
    AMQ_CATCH_RETHROW( ActiveMQException )
    AMQ_CATCHALL_THROW( ActiveMQException )
}

////////////////////////////////////////////////////////////////////////////////
cms::Session::AcknowledgeMode ActiveMQSession::getAcknowledgeMode(void) const
{
    return sessionInfo != NULL ? 
        sessionInfo->getAckMode() : Session::AUTO_ACKNOWLEDGE;
}

////////////////////////////////////////////////////////////////////////////////
bool ActiveMQSession::isTransacted(void) const
{
    return sessionInfo != NULL ? 
        sessionInfo->getAckMode() == Session::SESSION_TRANSACTED : false;
}

////////////////////////////////////////////////////////////////////////////////
void ActiveMQSession::acknowledge( ActiveMQConsumer* consumer,
                                   ActiveMQMessage* message )
    throw ( cms::CMSException )
{
    try
    {
        if( closed )
        {
            throw InvalidStateException(
                __FILE__, __LINE__,
                "ActiveMQSession::acknowledgeMessage - Session Already Closed" );
        }

        // Stores the Message and its consumer in the tranasction, if the
        // session is a transactional one.
        if( isTransacted() )
        {      
            transaction->addToTransaction( message, consumer );
        }

        // Delegate to connector to ack this message.
        return connection->getConnectionData()->
            getConnector()->acknowledge( 
                sessionInfo, dynamic_cast< cms::Message* >( message ) );
    }
    AMQ_CATCH_RETHROW( ActiveMQException )
    AMQ_CATCHALL_THROW( ActiveMQException )
}

////////////////////////////////////////////////////////////////////////////////
void ActiveMQSession::send( cms::Message* message, ActiveMQProducer* producer )
    throw ( cms::CMSException )
{
    try
    {
        if( closed )
        {
            throw InvalidStateException(
                __FILE__, __LINE__,
                "ActiveMQSession::onProducerClose - Session Already Closed" );
        }

        // Send via the connection
        connection->getConnectionData()->
            getConnector()->send( message, producer->getProducerInfo() );
    }
    AMQ_CATCH_RETHROW( ActiveMQException )
    AMQ_CATCHALL_THROW( ActiveMQException )
}

////////////////////////////////////////////////////////////////////////////////
void ActiveMQSession::onDestroySessionResource( 
    ActiveMQSessionResource* resource )
        throw ( cms::CMSException )
{
    try
    {
        if( closed )
        {
            throw InvalidStateException(
                __FILE__, __LINE__,
                "ActiveMQSession::onProducerClose - Session Already Closed");
        }

        ActiveMQConsumer* consumer = 
            dynamic_cast< ActiveMQConsumer*>( resource );

        if( consumer != NULL )
        {
            // Remove this Consumer from the Connection
            connection->removeMessageListener(
                consumer->getConsumerInfo()->getConsumerId() );

            // Remove this consumer from the Transaction if we are
            // transactional
            if( transaction != NULL )
            {
                transaction->removeFromTransaction( consumer );
            }
        }

        // Free its resources.
        connection->getConnectionData()->
            getConnector()->destroyResource( resource->getConnectorResource() );
    }
    AMQ_CATCH_RETHROW( ActiveMQException )
    AMQ_CATCHALL_THROW( ActiveMQException )
}

////////////////////////////////////////////////////////////////////////////////
cms::ExceptionListener* ActiveMQSession::getExceptionListener(void)
{
    if( connection != NULL )
    {
        return connection->getExceptionListener();
    }

    return NULL;
}
