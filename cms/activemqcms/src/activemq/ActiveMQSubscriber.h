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

#ifndef ACTIVEMQ_ACTIVEMQSUBSCRIBER_H_
#define ACTIVEMQ_ACTIVEMQSUBSCRIBER_H_

#include <cms/TopicSubscriber.h>
#include <cms/Topic.h>
#include <activemq/PendingMessagePool.h>
#include <cms/MessageListener.h>
#include <list>

namespace activemq{
	
	// Forward decalarations.
	class ActiveMQSession;
	
	class ActiveMQSubscriber
	: 
		public cms::TopicSubscriber,
		public cms::MessageListener
	{
	public:
	
		ActiveMQSubscriber( const cms::Topic* topic, ActiveMQSession* session );
		virtual ~ActiveMQSubscriber();
		
		/**
		 * Gets the message consumer's MessageListener.
		 */
		virtual cms::MessageListener* getMessageListener() const 
			throw( cms::CMSException ){
			return messageListener;
		}
		
		/**
		 * Sets the message consumer's MessageListener.
		 */
		virtual void setMessageListener( cms::MessageListener* listener ) 
			throw( cms::CMSException ){
			messageListener = listener;
		}
		
		/**
		 * Receives the next message produced for this message consumer.
		 */
		virtual cms::Message* receive() throw( cms::CMSException );
		
		/**
		 * Receives the next message that arrives within the specified 
		 * timeout interval.
		 * @param timeout The timeout value (in milliseconds)
		 */
		virtual cms::Message* receive( long timeout ) throw( cms::CMSException );
		
		/**
		 * Receives the next message if one is immediately available.
		 */
		virtual cms::Message* receiveNoWait() throw( cms::CMSException );
		
		/**
		 * Gets the Topic associated with this subscriber.
		 */
		virtual const cms::Topic* getTopic() const throw( cms::CMSException ){
			return topic;
		}
		
		/**
		 * Closes this object and deallocates the appropriate resources.
		 */
		virtual void close() throw( cms::CMSException );
		
		virtual void onMessage( const cms::Message* msg );
		
	private:
	
		void notify( const cms::Message* msg );
		
	private:
	
		cms::MessageListener* messageListener;
		const cms::Topic* topic;
		ActiveMQSession* session;
		PendingMessagePool pendingMessagePool;
		static const long readSleepMicroseconds = 10000;
	};
	
}

#endif /*ACTIVEMQ_ACTIVEMQSUBSCRIBER_H_*/
