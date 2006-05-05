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

#ifndef _CMS_MESSAGECONSUMER_H_
#define _CMS_MESSAGECONSUMER_H_
 
#include <cms/CMSException.h>
#include <cms/Closeable.h>
#include <string>

namespace cms{
	
	// Forward declarations.
	class MessageListener;
	class Message;
	
	/**
	 * A client uses a MessageConsumer object to receive messages from 
	 * a destination. A MessageConsumer object is created by passing a 
	 * Destination object to a message-consumer creation method supplied 
	 * by a session.
	 */
	class MessageConsumer : public Closeable{
		
	public:
	
		virtual ~MessageConsumer(){}
		
		/**
		 * Gets the message consumer's MessageListener.
		 */
		virtual MessageListener* getMessageListener() const 
			throw( CMSException ) = 0;
		
		/**
		 * Sets the message consumer's MessageListener.
		 */
		virtual void setMessageListener( MessageListener* listener ) 
			throw( CMSException ) = 0;
		
		/**
		 * Receives the next message produced for this message consumer.
		 */
		virtual Message* receive() throw( CMSException ) = 0;
		
		/**
		 * Receives the next message that arrives within the specified 
		 * timeout interval.
		 * @param timeout The timeout value (in milliseconds)
		 */
		virtual Message* receive( long timeout ) throw( CMSException ) = 0;
		
		/**
		 * Receives the next message if one is immediately available.
		 */
		virtual Message* receiveNoWait() throw( CMSException ) = 0;
	};
}

#endif /*_CMS_MESSAGECONSUMER_H_*/
