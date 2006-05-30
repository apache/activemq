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

#ifndef ACTIVEMQ_TRANSPORT_TRANSPORT_H_
#define ACTIVEMQ_TRANSPORT_TRANSPORT_H_

#include <cms/Service.h>
#include <cms/Closeable.h>
#include <cms/Topic.h>
#include <cms/MessageListener.h>
#include <activemq/transport/TopicListener.h>
#include <cms/ExceptionListener.h>

namespace activemq{
namespace transport{

	
    /**
     * Interface for a transport layer to a broker.
     * The protocol that is used to talk to the broker
     * is abstracted away from the client.
     * @author Nathan Mittler
     */
	class Transport
    : 
        public cms::Service,
        public cms::Closeable
    {		
	public:
		
		virtual ~Transport(){}
        
        /**
         * Disconnects from the broker.
         */
        virtual void close() throw (cms::CMSException) = 0;
        
        /**
         * Connects if necessary and starts the flow of messages to observers.
         */
        virtual void start() throw( cms::CMSException ) = 0;
        
        /**
         * Stops the flow of messages to observers.  Messages
         * will not be saved, so messages arriving after this call
         * will be lost.
         */
        virtual void stop() throw( cms::CMSException ) = 0;
        
        /**
         * Sends a message to the broker on the given topic.
         * @param topic The topic on which to send the message.
         * @param message The message to send.
         */
        virtual void sendMessage( const cms::Topic* topic, const cms::Message* message ) = 0;
		
        /**
         * Adds a message listener to a topic.
         * @param topic The topic to be observed.
         * @param listener The observer of messages on the topic.
         */
		virtual void addMessageListener( const cms::Topic* topic,
            cms::MessageListener* listener ) = 0;
            
        /**
         * Removes a message listener to a topic.
         * @param topic The topic to be observed.
         * @param listener The observer of messages on the topic.
         */  
        virtual void removeMessageListener( const cms::Topic* topic,
            cms::MessageListener* listener ) = 0;
            
        /**
         * Sets the observer of transport exceptions.
         * @param listener The listener to transport exceptions.
         */
        virtual void setExceptionListener( cms::ExceptionListener* listener ) = 0;
		
	};
    
}}

#endif /*ACTIVEMQ_TRANSPORT_TRANSPORT_H_*/
