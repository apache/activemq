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
#ifndef _ACTIVEMQ_CONNECTOR_CONNECTOR_H_
#define _ACTIVEMQ_CONNECTOR_CONNECTOR_H_

#include <list>

#include <cms/Startable.h>
#include <cms/Closeable.h>
#include <cms/MessageListener.h>
#include <cms/ExceptionListener.h>
#include <cms/Topic.h>
#include <cms/Queue.h>
#include <cms/TemporaryTopic.h>
#include <cms/TemporaryQueue.h>
#include <cms/Session.h>
#include <cms/BytesMessage.h>
#include <cms/TextMessage.h>
#include <cms/MapMessage.h>

#include <activemq/exceptions/InvalidStateException.h>

#include <activemq/transport/Transport.h>
#include <activemq/connector/SessionInfo.h>
#include <activemq/connector/ConsumerInfo.h>
#include <activemq/connector/ProducerInfo.h>
#include <activemq/connector/TransactionInfo.h>
#include <activemq/connector/ConsumerMessageListener.h>
#include <activemq/connector/ConnectorException.h>

namespace activemq{
namespace connector{

    // Forward declarations.
    class Connector 
    : 
        public cms::Startable,
        public cms::Closeable
    {
    public:    // Connector Types
    
        enum AckType
        {
            DeliveredAck = 0,  // Message delivered but not consumed
            PoisonAck    = 1,  // Message could not be processed due to 
                               // poison pill but discard anyway
            ConsumedAck  = 2   // Message consumed, discard            
        };
    
    public:
   
   	    virtual ~Connector(void) {};
        
        /**
         * Gets the Client Id for this connection, if this
         * connection has been closed, then this method returns ""
         * @return Client Id String
         */
        virtual std::string getClientId(void) const = 0;

        /**
         * Gets the Username for this connection, if this
         * connection has been closed, then this method returns ""
         * @return Username String
         */
        virtual std::string getUsername(void) const = 0;
        
        /**
         * Gets the Password for this connection, if this
         * connection has been closed, then this method returns ""
         * @return Password String
         */
        virtual std::string getPassword(void) const = 0;

        /**
         * Gets a reference to the Transport that this connection
         * is using.
         * @param reference to a transport
         * @throws InvalidStateException if the Transport is not set
         */
        virtual transport::Transport& getTransport(void) const 
            throw (exceptions::InvalidStateException ) = 0;

        /**
         * Creates a Session Info object for this connector
         * @param Acknowledgement Mode of the Session
         * @returns Session Info Object
         * @throws ConnectorException
         */
        virtual SessionInfo* createSession(
            cms::Session::AcknowledgeMode ackMode ) 
                throw( ConnectorException ) = 0;
      
        /** 
         * Create a Consumer for the given Session
         * @param Destination to Subscribe to.
         * @param Session Information.
         * @return Consumer Information
         * @throws ConnectorException
         */
        virtual ConsumerInfo* createConsumer(
            const cms::Destination* destination, 
            SessionInfo* session,
            const std::string& selector = "" )
                throw ( ConnectorException ) = 0;
         
        /** 
         * Create a Durable Consumer for the given Session
         * @param Topic to Subscribe to.
         * @param Session Information.
         * @param name of the Durable Topic
         * @param Selector
         * @param if set, inhibits the delivery of messages 
         *        published by its own connection 
         * @return Consumer Information
         * @throws ConnectorException
         */
        virtual ConsumerInfo* createDurableConsumer(
            const cms::Topic* topic, 
            SessionInfo* session,
            const std::string& name,
            const std::string& selector = "",
            bool noLocal = false )
                throw ( ConnectorException ) = 0;

        /** 
         * Create a Consumer for the given Session
         * @param Destination to Subscribe to.
         * @param Session Information.
         * @return Producer Information
         * @throws ConnectorException
         */
        virtual ProducerInfo* createProducer(
            const cms::Destination* destination, 
            SessionInfo* session )
                throw ( ConnectorException ) = 0;

        /**
         * Creates a Topic given a name and session info
         * @param Topic Name
         * @param Session Information
         * @return a newly created Topic Object
         * @throws ConnectorException
         */
        virtual cms::Topic* createTopic( const std::string& name, 
                                         SessionInfo* session )
            throw ( ConnectorException ) = 0;
          
        /**
         * Creates a Queue given a name and session info
         * @param Queue Name
         * @param Session Information
         * @return a newly created Queue Object
         * @throws ConnectorException
         */
        virtual cms::Queue* createQueue( const std::string& name, 
                                         SessionInfo* session )
            throw ( ConnectorException ) = 0;

        /**
         * Creates a Temporary Topic given a name and session info
         * @param Temporary Topic Name
         * @param Session Information
         * @return a newly created Temporary Topic Object
         * @throws ConnectorException
         */
        virtual cms::TemporaryTopic* createTemporaryTopic(
            SessionInfo* session )
                throw ( ConnectorException ) = 0;
          
        /**
         * Creates a Temporary Queue given a name and session info
         * @param Temporary Queue Name
         * @param Session Information
         * @return a newly created Temporary Queue Object
         * @throws ConnectorException
         */
        virtual cms::TemporaryQueue* createTemporaryQueue(
            SessionInfo* session )
                throw ( ConnectorException ) = 0;

        /**
         * Sends a Message
         * @param The Message to send.
         * @param Producer Info for the sender of this message
         * @throws ConnectorException
         */
        virtual void send( cms::Message* message, ProducerInfo* producerInfo ) 
            throw ( ConnectorException ) = 0;
      
        /**
         * Sends a set of Messages
         * @param List of Messages to send.
         * @param Producer Info for the sender of this message
         * @throws ConnectorException
         */
        virtual void send( std::list<cms::Message*>& messages,
                           ProducerInfo* producerInfo) 
            throw ( ConnectorException ) = 0;
         
        /**
         * Acknowledges a Message
         * @param An ActiveMQMessage to Ack.
         * @throws ConnectorException
         */
        virtual void acknowledge( const SessionInfo* session,
                                  const cms::Message* message,
                                  AckType ackType = ConsumedAck)
            throw ( ConnectorException ) = 0;

        /**
         * Starts a new Transaction.
         * @param Session Information
         * @throws ConnectorException
         */
        virtual TransactionInfo* startTransaction(
            SessionInfo* session ) 
                throw ( ConnectorException ) = 0;
         
        /**
         * Commits a Transaction.
         * @param The Transaction information
         * @param Session Information
         * @throws ConnectorException
         */
        virtual void commit( TransactionInfo* transaction, 
                             SessionInfo* session )
            throw ( ConnectorException ) = 0;

        /**
         * Rolls back a Transaction.
         * @param The Transaction information
         * @param Session Information
         * @throws ConnectorException
         */
        virtual void rollback( TransactionInfo* transaction, 
                               SessionInfo* session )
            throw ( ConnectorException ) = 0;

        /**
         * Creates a new Message.
         * @param Session Information
         * @param Transaction Info for this Message
         * @throws ConnectorException
         */
        virtual cms::Message* createMessage(
            SessionInfo* session,
            TransactionInfo* transaction )
                throw ( ConnectorException ) = 0;

        /**
         * Creates a new BytesMessage.
         * @param Session Information
         * @param Transaction Info for this Message
         * @throws ConnectorException
         */
        virtual cms::BytesMessage* createBytesMessage(
            SessionInfo* session,
            TransactionInfo* transaction )
                throw ( ConnectorException ) = 0;

        /**
         * Creates a new TextMessage.
         * @param Session Information
         * @param Transaction Info for this Message
         * @throws ConnectorException
         */
        virtual cms::TextMessage* createTextMessage(
            SessionInfo* session,
            TransactionInfo* transaction )
                throw ( ConnectorException ) = 0;

        /**
         * Creates a new MapMessage.
         * @param Session Information
         * @param Transaction Info for this Message
         * @throws ConnectorException
         */
        virtual cms::MapMessage* createMapMessage(
            SessionInfo* session,
            TransactionInfo* transaction )
                throw ( ConnectorException ) = 0;

        /** 
         * Unsubscribe from a givenDurable Subscription
         * @param name of the Subscription
         * @throws ConnectorException
         */
        virtual void unsubscribe( const std::string& name )
            throw ( ConnectorException ) = 0;

        /**
         * Destroys the given connector resource.
         * @param resource the resource to be destroyed.
         * @throws ConnectorException
         */
        virtual void destroyResource( ConnectorResource* resource )
            throw ( ConnectorException ) = 0;
            
        /** 
         * Sets the listener of consumer messages.
         * @param listener the observer.
         */
        virtual void setConsumerMessageListener(
            ConsumerMessageListener* listener ) = 0;

        /** 
         * Sets the Listner of exceptions for this connector
         * @param ExceptionListener the observer.
         */
        virtual void setExceptionListener(
            cms::ExceptionListener* listener ) = 0;
    };

}}

#endif /*_ACTIVEMQ_CONNECTOR_CONNECTOR_H_*/
