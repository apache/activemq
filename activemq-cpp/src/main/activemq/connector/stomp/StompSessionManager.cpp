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

#include "StompSessionManager.h"

#include <activemq/core/ActiveMQMessage.h>
#include <activemq/concurrent/Concurrent.h>
#include <activemq/connector/stomp/StompSessionInfo.h>
#include <activemq/connector/stomp/StompConsumerInfo.h>
#include <activemq/connector/stomp/commands/SubscribeCommand.h>
#include <activemq/connector/stomp/commands/UnsubscribeCommand.h>
#include <activemq/connector/stomp/StompSelector.h>

using namespace std;
using namespace activemq;
using namespace activemq::core;
using namespace activemq::exceptions;
using namespace activemq::transport;
using namespace activemq::connector;
using namespace activemq::connector::stomp;
using namespace activemq::connector::stomp::commands;

////////////////////////////////////////////////////////////////////////////////
StompSessionManager::StompSessionManager( const std::string& connectionId, 
                                          Transport* transport )
{
    if( transport == NULL )
    {
        throw NullPointerException( 
            __FILE__, __LINE__,
            "StompSessionManager::StompSessionManager" );
    }

    this->transport = transport;
    this->connectionId = connectionId;
    this->nextSessionId = 0;
    this->nextConsumerId = 0;
    this->messageListener = NULL;
}

////////////////////////////////////////////////////////////////////////////////
StompSessionManager::~StompSessionManager(void)
{
    // NOTE - I am not cleaning out the ConsumerInfo objects in the
    // map becaise it is really the job of the consumer ot remove itself
    // when it is destructed.  If it doesn't then we would have problems,
    // but if it does, but it's deleted after this object then we would
    // still have problems.  
}

////////////////////////////////////////////////////////////////////////////////
unsigned int StompSessionManager::getNextSessionId(void)
{
    synchronized(&mutex)
    {
        return nextSessionId++;
    }
    
    return 0;
}

////////////////////////////////////////////////////////////////////////////////
unsigned int StompSessionManager::getNextConsumerId(void)
{
    synchronized(&mutex)
    {
        return nextConsumerId++;
    }

    return 0;
}

////////////////////////////////////////////////////////////////////////////////
connector::SessionInfo* StompSessionManager::createSession(
    cms::Session::AcknowledgeMode ackMode) 
        throw ( exceptions::ActiveMQException )
{
    try
    {
        SessionInfo* session = new StompSessionInfo();
        
        // Init data
        session->setAckMode(ackMode);
        session->setConnectionId( connectionId );
        session->setSessionId( getNextSessionId() );
        
        return session;
    }
    AMQ_CATCH_RETHROW( ActiveMQException )
    AMQ_CATCHALL_THROW( ActiveMQException )
}

////////////////////////////////////////////////////////////////////////////////
void StompSessionManager::removeSession( 
    connector::SessionInfo* session )
        throw ( exceptions::ActiveMQException )
{
    // NO-op
}
    
////////////////////////////////////////////////////////////////////////////////
connector::ConsumerInfo* StompSessionManager::createConsumer(
    cms::Destination* destination, 
    SessionInfo* session,
    const std::string& selector)
        throw( ConnectorException )
{
    try
    {
        // Delegate to the createDurableConsumer method, just pas the
        // appropriate params so that a regular consumer is created on
        // the broker side.
        return createDurableConsumer( 
            destination, session, "", selector, false );    
    }
    AMQ_CATCH_RETHROW( ConnectorException )
    AMQ_CATCHALL_THROW( ConnectorException )
}

////////////////////////////////////////////////////////////////////////////////
connector::ConsumerInfo* StompSessionManager::createDurableConsumer(
    cms::Destination* destination, 
    SessionInfo* session,
    const std::string& name,
    const std::string& selector,
    bool noLocal )
        throw ( ConnectorException )
{
    try
    {
        synchronized(&mutex)
        {
            // Find the right mapping to consumers        
            ConsumerMap& consumerMap = 
                destinationMap[ destination->toString() ];
            
            // We only need to send a sub request if there are no active 
            // consumers on this destination.  
            if( consumerMap.empty() )
            {
                // Send the request to the Broker
                SubscribeCommand cmd;
                
                if( session->getAckMode() == cms::Session::ClientAcknowledge )
                {
                    cmd.setAckMode( CommandConstants::ACK_CLIENT );
                }
                cmd.setDestination( destination->toProviderString() );
                cmd.setNoLocal( noLocal );

                if( name != "" )
                {
                    cmd.setSubscriptionName( name );
                }
                
                // The Selector is set on the first subscribe on this dest,
                // and if another consumer is created on this destination
                // that specifies a selector it will be ignored.  While 
                // this is not ideal, is the only way to handle the fact
                // that activemq stomp doesn't support multiple sessions.
                if( selector != "" )
                {
                    cmd.setMessageSelector( selector );
                }
        
                // Fire the message        
                transport->oneway( &cmd );
            }
             
            // Initialize a new Consumer info Message
            ConsumerInfo* consumer = new StompConsumerInfo();
            
            consumer->setConsumerId( getNextConsumerId() );
            consumer->setDestination( *destination );
            consumer->setMessageSelector( selector );
            consumer->setSessionInfo( session );
    
            // Store this consumer for later message dispatching.        
            consumerMap.insert( 
                make_pair( consumer->getConsumerId(), consumer ) );
            
            return consumer;
        }
        
        return NULL;
    }
    AMQ_CATCH_RETHROW( ConnectorException )
    AMQ_CATCHALL_THROW( ConnectorException )
}

////////////////////////////////////////////////////////////////////////////////
void StompSessionManager::removeConsumer(
    connector::ConsumerInfo* consumer)
        throw( ConnectorException )
{
    try
    {
        synchronized(&mutex)
        {
            DestinationMap::iterator itr = 
                destinationMap.find( consumer->getDestination().toString() );
                
            if( itr == destinationMap.end() )
            {
                // Already removed from the map
                return;
            }
            
            ConsumerMap& consumers = itr->second;
            
            // Remove from the map.
            consumers.erase( consumer->getConsumerId() );
            
            // If there are no more on this destination then we unsubscribe
            if( consumers.empty() )
            {
                UnsubscribeCommand cmd;
                
                cmd.setDestination( 
                    consumer->getDestination().toProviderString() );
                
                // Send the message
                transport->oneway( &cmd );
            }    
        }
    }
    AMQ_CATCH_RETHROW( ConnectorException )
    AMQ_CATCHALL_THROW( ConnectorException )
}

////////////////////////////////////////////////////////////////////////////////
void StompSessionManager::onStompCommand( commands::StompCommand* command ) 
    throw ( StompConnectorException )
{
    try
    {
        cms::Message* message = dynamic_cast< cms::Message*>( command );

        if( message == NULL )
        {
            throw StompConnectorException(
                __FILE__, __LINE__,
                "StompSessionManager::onStompCommand - Invalid Command" );
        }

        if( messageListener == NULL )
        {
            throw StompConnectorException(
                __FILE__, __LINE__,
                "StompSessionManager::onStompCommand - "
                "No Message Listener Registered." );
        }
                
        synchronized(&mutex)
        {
            DestinationMap::iterator itr = 
                destinationMap.find( message->getCMSDestination().toString() );

            if( itr == destinationMap.end() )
            {
                throw StompConnectorException(
                    __FILE__, __LINE__,
                    "StompSessionManager::onStompCommand - "
                    "Received a Message that doesn't have a listener" );
            }

            // If we only have 1 consumer, we don't need to clone the original
            // message.
            if(itr->second.size() == 1)
            {
                ConsumerInfo* consumerInfo = itr->second.begin()->second;
                
                if( StompSelector::isSelected( 
                        consumerInfo->getMessageSelector(),
                        message ) )
                {                    
                    ActiveMQMessage* msg = 
                        dynamic_cast< ActiveMQMessage* >( message );
                    messageListener->onConsumerMessage( consumerInfo, msg );
                }
                
                return;
            }

            // We have more than one consumer of this message - we have to
            // clone the message for each consumer so they don't destroy each other's
            // message.
            ConsumerMap::iterator c_itr = itr->second.begin();
            
            for(; c_itr != itr->second.end(); ++c_itr )
            {
                ConsumerInfo* consumerInfo = c_itr->second;
                
                if( StompSelector::isSelected( 
                        consumerInfo->getMessageSelector(),
                        message ) )
                {
                    ActiveMQMessage* msg = 
                        dynamic_cast< ActiveMQMessage* >( message->clone() );
                    messageListener->onConsumerMessage( consumerInfo, msg );
                }
            }
            
            // We got here which means that we sent copies, so remove
            // the original.
            delete command;
        }
    }
    AMQ_CATCH_RETHROW( StompConnectorException )
    AMQ_CATCH_EXCEPTION_CONVERT( ActiveMQException, StompConnectorException )
    AMQ_CATCHALL_THROW( StompConnectorException )
}
