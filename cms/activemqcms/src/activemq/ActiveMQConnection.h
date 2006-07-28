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

#ifndef ACTIVEMQ_ACTIVEMQCONNECTION_H_
#define ACTIVEMQ_ACTIVEMQCONNECTION_H_
 
#include <cms/TopicConnection.h>
#include <cms/TopicSession.h>
#include <cms/ExceptionListener.h>
#include <activemq/transport/Transport.h>
#include <vector>

namespace activemq{
	
	/**
	 * Defines a connection for interfacing with an
	 * ActiveMQ broker.
	 * @author Nathan Mittler
	 */
	class ActiveMQConnection 
	: 
		public cms::ExceptionListener,
		public cms::TopicConnection
	{
	public:
	
		/**
		 * Constructor
		 * @param transport The transport channel.
		 */
		ActiveMQConnection( transport::Transport* transport );
			
		/**
		 * Destructor.
		 */
		virtual ~ActiveMQConnection();
		
		/**
		 * Begins the dispatch of messages on this connection.
		 */
		virtual void start() throw( cms::CMSException );
		
		/**
		 * Stops the dispatch of incoming messages to listeners.  Calling
		 * start again will resume the flow of messages.
		 */
		virtual void stop() throw( cms::CMSException );
		
		/**
		 * Closes this connection and all child sessions.
		 */
		virtual void close() throw( cms::CMSException );	
		
		/**
		 * Delegates to createTopicSession().
		 */
		virtual cms::Session* createSession( const bool transacted,
			const cms::Session::AcknowledgeMode acknowledgeMode = cms::Session::AUTO_ACKNOWLEDGE ) 
			throw( cms::CMSException )
		{
			return createTopicSession( transacted, acknowledgeMode );
		}
			
		/**
		 * Creates a topic session for this connection.
		 */
		virtual cms::TopicSession* createTopicSession( const bool transacted, 
			const cms::Session::AcknowledgeMode acknowledgeMode = cms::Session::AUTO_ACKNOWLEDGE) 
			throw( cms::CMSException );		
			
		/**
		 * Sets the listener to exceptions of this connection.
		 */
		virtual void setExceptionListener( cms::ExceptionListener* listener )
			throw( cms::CMSException )
		{
			exceptionListener = listener;
		}
		
		/**
		 * Gets the listener of exceptions of this connection.
		 */
		virtual cms::ExceptionListener* getExceptionListener() const
			throw( cms::CMSException )
		{
			return exceptionListener;
		}		
		
		virtual const char* getUserName() const{
			return userName.c_str();
		}
		
		virtual const char* getPassword() const{
			return password.c_str();
		}
		
		/**
		 * Gets the transport channel for this connection.
		 */
		virtual transport::Transport* getTransportChannel(){
			return transport;
		}
		
		/**
		 * Gets the transport channel for this connection.
		 */
		virtual const transport::Transport* getTransportChannel() const{
			return transport;
		}
		
        /**
         * Called by the transport layer when an exception occurs.
         * @param exception The exception.
         */
		virtual void onException( const cms::CMSException* exception );
		
	private:	
		
		/**
		 * The transport channel.
		 */
		transport::Transport* transport;
		
		/**
		 * The user name for connecting to the broker.
		 */
		std::string userName;
		
		/**
		 * The password for connecting to the broker.
		 */
		std::string password;
		
		/**
		 * The listener of exceptions from this connection.
		 */
		cms::ExceptionListener* exceptionListener;
	};
	
}

#endif /*ACTIVEMQ_ACTIVEMQCONNECTION_H_ */

