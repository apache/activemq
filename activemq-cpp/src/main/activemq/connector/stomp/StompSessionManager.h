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

#ifndef _ACTIVEMQ_CONNECTOR_STOMP_STOMPSESSIONMANAGER_H_
#define _ACTIVEMQ_CONNECTOR_STOMP_STOMPSESSIONMANAGER_H_

#include <activemq/connector/SessionInfo.h>
#include <activemq/connector/ConsumerInfo.h>
#include <activemq/transport/Transport.h>
#include <activemq/concurrent/Mutex.h>
#include <activemq/connector/ConnectorException.h>
#include <activemq/connector/stomp/StompCommandListener.h>
#include <activemq/connector/ConsumerMessageListener.h>
#include <activemq/connector/stomp/commands/SubscribeCommand.h>

namespace activemq{
namespace connector{
namespace stomp{
    
    /**
     * The Stomp Session Manager is responsible for managing multiple
     * Client Sessions.  The management involves routing messages to 
     * sessions.  If more than one ActiveMQConsumer is created that is
     * listening to the same Topic or Queue, then the messages that are
     * received must be delivered to each of those sessions, and copied
     * so that a transactional session can manage the lifetime of the
     * message.
     */
    class StompSessionManager : public StompCommandListener
    {
    private:
    
        // Map Types
        typedef std::map< unsigned int, ConsumerInfo* > ConsumerMap;
        typedef std::map< std::string, ConsumerMap >    DestinationMap;        

    private:
    
        // Next id to be used for a Session Id
        unsigned int nextSessionId;
        
        // Next id to be used for a Consumer Id
        unsigned int nextConsumerId;
        
        // Mutex to protect ids.
        concurrent::Mutex mutex;
        
        // Mapping of a Session to all the consumer's
        DestinationMap destinationMap;
        
        // Transport that we use to find a transport for sending
        // commands
        transport::Transport* transport;
        
        // Consumer Message listener, we notify this whenever we receive 
        // a new StompMessage type command.
        ConsumerMessageListener* messageListener;
        
        // The global connection id
        std::string connectionId;
        
    public:

    	StompSessionManager( const std::string& connectionId, 
                             transport::Transport* transport );
    	virtual ~StompSessionManager(void);

        /**
         * Creates a new Session and returns a SessionInfo object whose
         * lifetime is the property of the caller.
         * @param the ackMode of the session.
         * @return new SessionInfo object
         */
        virtual connector::SessionInfo* createSession(
            cms::Session::AcknowledgeMode ackMode )
                throw ( exceptions::ActiveMQException );

        /**
         * removes the specified Session from the Manager, all data that
         * is associated with session consumers is now lost.  The session
         * is not deleted here, it is the owner's responsibility.
         * @param the session info for the session to remove.
         */
        virtual void removeSession( connector::SessionInfo* session )
            throw ( exceptions::ActiveMQException );
        
        /**
         * Creates a new consumer to the specified session, will subscribe
         * to the destination if another consumer hasn't already been 
         * subbed to that destination.  The returned consumer is the 
         * owned by the caller and not deleted by this class.
         * @param destination to subscribe to
         * @param session to associate with
         * @param selector string
         * @return new ConsumerInfo object.
         */
        virtual connector::ConsumerInfo* createConsumer(
            const cms::Destination* destination, 
            SessionInfo* session,
            const std::string& selector )
                throw( StompConnectorException );

        /**
         * Creates a new durable consumer to the specified session, will 
         * subscribe to the destination if another consumer hasn't already 
         * been subbed to that destination.  The returned consumer is the 
         * owned by the caller and not deleted by this class.
         * @param Topic to subscribe to
         * @param session to associate with
         * @param Subscription Name
         * @param selector string
         * @param Should we be notified of messages we send.
         * @return new ConsumerInfo object.
         */
        virtual connector::ConsumerInfo* createDurableConsumer(
            const cms::Destination* destination, 
            SessionInfo* session,
            const std::string& name,
            const std::string& selector,
            bool noLocal )
                throw ( StompConnectorException );

        /**
         * Removes the Consumer from the session, will unsubscrive if the
         * consumer is the only one listeneing on this destination.  The
         * Consumer is not deleted, just unassociated from the Manager
         * caller is responsible for managing the lifetime.
         * @param the ConsumerInfo for the consumer to remove
         * @throws ConnectorException
         */            
        virtual void removeConsumer( connector::ConsumerInfo* consumer )
            throw( StompConnectorException );

        /** 
         * Sets the listener of consumer messages.
         * @param listener the observer.
         */
        virtual void setConsumerMessageListener(
            ConsumerMessageListener* listener )
        {
            this->messageListener = listener;
        }

    public:   // StompCommand Listener
    
        /**
         * Process the Stomp Command
         * @param command to process
         * @throw ConnterException
         */
        virtual void onStompCommand( commands::StompCommand* command ) 
            throw ( StompConnectorException );
            
    protected: 
    
        /**
         * Sets Subscribe Command options from the properties of a 
         * destination object.
         * @param The destination that we are subscribing to.
         * @param The pending Subscribe command
         */
        virtual void setSubscribeOptions( const cms::Destination* dest,
                                          commands::SubscribeCommand& command )
            throw ( StompConnectorException );
            
    protected:
    
        /**
         * Gets the Next Session Id
         * @return unique session id
         */
        virtual unsigned int getNextSessionId(void);
        
        /**
         * Gets the Next Session Id
         * @return unique session id
         */
        virtual unsigned int getNextConsumerId(void);

    };

}}}

#endif /*_ACTIVEMQ_CONNECTOR_STOMP_STOMPSESSIONMANAGER_H_*/
