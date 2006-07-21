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
#ifndef _ACTIVEMQ_CORE_ACTIVEMQSESSION_H_
#define _ACTIVEMQ_CORE_ACTIVEMQSESSION_H_

#include <cms/Session.h>
#include <cms/ExceptionListener.h>
#include <activemq/connector/SessionInfo.h>
#include <activemq/core/ActiveMQSessionResource.h>

namespace activemq{
namespace core{

    class ActiveMQTransaction;
    class ActiveMQConnection;
    class ActiveMQConsumer;
    class ActiveMQMessage;
    class ActiveMQProducer;
    class ActiveMQConsumer;
   
    class ActiveMQSession : public cms::Session
    {
    private:
   
        // SessionInfo for this Session
        connector::SessionInfo* sessionInfo;
      
        // Transaction Management object
        ActiveMQTransaction* transaction;
      
        // Connection
        ActiveMQConnection* connection;
      
        // Bool to indicate if this session was closed.
        bool closed;
      
    public:
   
        ActiveMQSession( connector::SessionInfo* sessionInfo,
                         const util::Properties& properties,
                         ActiveMQConnection* connection );
   
        virtual ~ActiveMQSession(void);
   
    public:   // Implements Mehtods
   
        /**
         * Closes the Session
         * @throw CMSException
         */
        virtual void close(void) throw ( cms::CMSException );
      
        /**
         * Commits all messages done in this transaction and releases any 
         * locks currently held.
         * @throws CMSException
         */
        virtual void commit(void) throw ( cms::CMSException );

        /**
         * Rollsback all messages done in this transaction and releases any 
         * locks currently held.
         * @throws CMSException
         */
        virtual void rollback(void) throw ( cms::CMSException );

        /**
         * Creates a MessageConsumer for the specified destination.
         * @param the Destination that this consumer receiving messages for.
         * @throws CMSException
         */
        virtual cms::MessageConsumer* createConsumer(
            const cms::Destination* destination )
                throw ( cms::CMSException );

        /**
         * Creates a MessageConsumer for the specified destination, using a 
         * message selector.
         * @param the Destination that this consumer receiving messages for.
         * @throws CMSException
         */
        virtual cms::MessageConsumer* createConsumer(
            const cms::Destination* destination,
            const std::string& selector )
                throw ( cms::CMSException );
         
        /**
         * Creates a durable subscriber to the specified topic, using a 
         * message selector
         * @param the topic to subscribe to
         * @param name used to identify the subscription
         * @param only messages matching the selector are received
         * @throws CMSException
         */
        virtual cms::MessageConsumer* createDurableConsumer(
            const cms::Topic* destination,
            const std::string& name,
            const std::string& selector,
            bool noLocal = false )
                throw ( cms::CMSException );

        /**
         * Creates a MessageProducer to send messages to the specified 
         * destination.
         * @param the Destination to publish on
         * @throws CMSException
         */
        virtual cms::MessageProducer* createProducer(
            const cms::Destination* destination )
                throw ( cms::CMSException );
         
        /**
         * Creates a queue identity given a Queue name.
         * @param the name of the new Queue
         * @throws CMSException
         */
        virtual cms::Queue* createQueue( const std::string& queueName )
            throw ( cms::CMSException );
      
        /**
         * Creates a topic identity given a Queue name.
         * @param the name of the new Topic
         * @throws CMSException
         */
        virtual cms::Topic* createTopic( const std::string& topicName )
            throw ( cms::CMSException );

        /**
         * Creates a TemporaryQueue object.
         * @throws CMSException
         */
        virtual cms::TemporaryQueue* createTemporaryQueue(void)
            throw ( cms::CMSException );

        /**
         * Creates a TemporaryTopic object.
         * @throws CMSException
         */
        virtual cms::TemporaryTopic* createTemporaryTopic(void)
            throw ( cms::CMSException );
         
        /**
         * Creates a new Message
         * @throws CMSException
         */
        virtual cms::Message* createMessage(void) 
            throw ( cms::CMSException );

        /**
         * Creates a BytesMessage
         * @throws CMSException
         */
        virtual cms::BytesMessage* createBytesMessage(void) 
            throw ( cms::CMSException );

        /**
         * Creates a BytesMessage and sets the paylod to the passed value
         * @param an array of bytes to set in the message
         * @param the size of the bytes array, or number of bytes to use
         * @throws CMSException
         */
        virtual cms::BytesMessage* createBytesMessage( 
            const unsigned char* bytes,
            unsigned long bytesSize ) 
                throw ( cms::CMSException );

        /**
         * Creates a new TextMessage
         * @throws CMSException
         */
        virtual cms::TextMessage* createTextMessage(void) 
            throw ( cms::CMSException );
      
        /**
         * Creates a new TextMessage and set the text to the value given
         * @param the initial text for the message
         * @throws CMSException
         */
        virtual cms::TextMessage* createTextMessage( const std::string& text ) 
            throw ( cms::CMSException );

        /**
         * Creates a new TextMessage
         * @throws CMSException
         */
        virtual cms::MapMessage* createMapMessage(void) 
            throw ( cms::CMSException );

        /**
         * Returns the acknowledgement mode of the session.
         * @return the Sessions Acknowledge Mode
         */
        virtual cms::Session::AcknowledgeMode getAcknowledgeMode(void) const;
      
        /**
         * Gets if the Sessions is a Transacted Session
         * @return transacted true - false.
         */
        virtual bool isTransacted(void) const;
          
   public:   // ActiveMQSession specific Methods
   
        /**
         * Sends a message from the Producer specified
         * @param cms::Message pointer
         * @param Producer Information
         * @throws CMSException
         */
        virtual void send( cms::Message* message, ActiveMQProducer* producer )
            throw ( cms::CMSException );
         
        /**
         * When a ActiveMQ core object is closed or destroyed it should call 
         * back and let the session know that it is going away, this allows 
         * the session to clean up any associated resources.  This method 
         * destroy's the data that is associated with a Producer object
         * @param The Producer that is being destoryed
         * @throw CMSException
         */
        virtual void onDestroySessionResource( ActiveMQSessionResource* resource )
            throw ( cms::CMSException );

        /**
         * Called to acknowledge the receipt of a message.  
         * @param The consumer that received the message
         * @param The Message to acknowledge.
         * @throws CMSException
         */
        virtual void acknowledge( ActiveMQConsumer* consumer,
                                  ActiveMQMessage* message )
            throw ( cms::CMSException );
         
        /**
         * This method gets any registered exception listener of this sessions
         * connection and returns it.  Mainly intended for use by the objects
         * that this session creates so that they can notify the client of
         * exceptions that occur in the context of another thread.
         * @returns cms::ExceptionListener pointer or NULL
         */
        virtual cms::ExceptionListener* getExceptionListener(void);

        /**
         * Gets the Session Information object for this session, if the
         * session is closed than this returns null
         * @return SessionInfo Pointer
         */
        virtual connector::SessionInfo* getSessionInfo(void) {
            return sessionInfo;
        }
      
   };

}}

#endif /*_ACTIVEMQ_CORE_ACTIVEMQSESSION_H_*/
