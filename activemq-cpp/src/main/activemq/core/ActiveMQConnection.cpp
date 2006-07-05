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

#include "ActiveMQConnection.h"

#include <cms/Session.h>
#include <activemq/core/ActiveMQSession.h>
#include <activemq/core/ActiveMQConsumer.h>
#include <activemq/exceptions/NullPointerException.h>

using namespace cms;
using namespace activemq;
using namespace activemq::core;
using namespace activemq::connector;
using namespace activemq::exceptions;
using namespace std;

////////////////////////////////////////////////////////////////////////////////
ActiveMQConnection::ActiveMQConnection(ActiveMQConnectionData* connectionData)
{
    this->connectionData = connectionData;
    this->started = false;
    this->exceptionListener = NULL;

    // We want to be the sink for all messages from the Connector
    connectionData->getConnector()->setConsumerMessageListener( this );
}

////////////////////////////////////////////////////////////////////////////////
ActiveMQConnection::~ActiveMQConnection(void)
{
    try
    {
        close();
    }
    AMQ_CATCH_NOTHROW( ActiveMQException )
    AMQ_CATCHALL_NOTHROW( )
}

////////////////////////////////////////////////////////////////////////////////
cms::Session* ActiveMQConnection::createSession(void) 
    throw ( cms::CMSException )
{
    try
    {
        return this->createSession( Session::AutoAcknowledge );
    }
    AMQ_CATCH_RETHROW( ActiveMQException )
    AMQ_CATCHALL_THROW( ActiveMQException )
}

////////////////////////////////////////////////////////////////////////////////
cms::Session* ActiveMQConnection::createSession(
   cms::Session::AcknowledgeMode ackMode ) 
      throw ( cms::CMSException )
{
    try
    {
        return new ActiveMQSession(
            connectionData->getConnector()->createSession( ackMode ), 
            connectionData->getProperties(),
            this );
    }
    AMQ_CATCH_RETHROW( ActiveMQException )
    AMQ_CATCHALL_THROW( ActiveMQException )
}
   
////////////////////////////////////////////////////////////////////////////////
std::string ActiveMQConnection::getClientId(void) const
{
   return connectionData->getConnector()->getClientId();
}

////////////////////////////////////////////////////////////////////////////////
void ActiveMQConnection::close(void) throw ( cms::CMSException )
{
    try
    {
        // Once current deliveries are done this stops the delivery 
        // of any new messages.
        started = false;
    
        // Destroy the connection data
        delete connectionData;
        connectionData = NULL;
    }
    AMQ_CATCH_RETHROW( ActiveMQException )
    AMQ_CATCHALL_THROW( ActiveMQException )
}

////////////////////////////////////////////////////////////////////////////////
void ActiveMQConnection::start(void) throw ( cms::CMSException )
{
    // This starts or restarts the delivery of all incomming messages
    // messages delivered while this connection is stopped are dropped
    // and not acknowledged.
    started = true;
}

////////////////////////////////////////////////////////////////////////////////
void ActiveMQConnection::stop(void) throw ( cms::CMSException )
{
    // Once current deliveries are done this stops the delivery of any
    // new messages.
    started = false;
}
   
////////////////////////////////////////////////////////////////////////////////
void ActiveMQConnection::addMessageListener( const unsigned int consumerId,
                                             ActiveMQMessageListener* listener )
{
    // Place in Map
    synchronized(&mutex)
    {
        consumers[consumerId] = listener;
    }
}
  
////////////////////////////////////////////////////////////////////////////////
void ActiveMQConnection::removeMessageListener( const unsigned int consumerId )
{
    // Remove from Map
    synchronized(&mutex)
    {
        consumers.erase( consumerId );
    }
}

////////////////////////////////////////////////////////////////////////////////
void ActiveMQConnection::onConsumerMessage( connector::ConsumerInfo* consumer,
                                            core::ActiveMQMessage* message )
{
    try
    {
        if( connectionData == NULL)
        {
            NullPointerException ex(
                __FILE__, __LINE__,
                "ActiveMQConnection::onConsumerMessage - "
                "Connection Data Null, could be closed." );

            fire( ex );

            return;
        }

        // When not started we drop incomming messages
        if( !started )
        {
            // Indicate to Broker that we received the message, but it
            // was not consumed.
            connectionData->getConnector()->acknowledge(
                consumer->getSessionInfo(), 
                (Message*)message, 
                Connector::DeliveredAck );
                
            // Delete the message here
            delete message;
            
            return;
        }
        
        // Started, so lock map and dispatch the message.
        synchronized(&mutex)
        {
            if(consumers.find(consumer->getConsumerId()) != consumers.end())
            {
                consumers[consumer->getConsumerId()]->
                    onActiveMQMessage( message );
            }
        }        
    }
    catch( exceptions::ActiveMQException& ex )
    {
        ex.setMark( __FILE__, __LINE__ );
        fire( ex );
    }
    catch( ... )
    {
        exceptions::ActiveMQException ex( 
           __FILE__, __LINE__, 
           "IOTransport::run - caught unknown exception" );
        fire( ex );            
    }

}                                             

