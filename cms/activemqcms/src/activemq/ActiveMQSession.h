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

#ifndef ACTIVEMQ_ACTIVEMQSESSION_H_
#define ACTIVEMQ_ACTIVEMQSESSION_H_

#include <cms/TopicSession.h>
#include <activemq/ActiveMQException.h>

namespace activemq{
	
	// Forward declarations.
	class ActiveMQConnection;
	
	class ActiveMQSession : public cms::TopicSession
	{
	public:
	
		ActiveMQSession( 
			ActiveMQConnection* connection,
			const cms::Session::AcknowledgeMode acknowledgeMode );
		virtual ~ActiveMQSession();		
		
		/**
		 * Closes this session.  The connection will still be active.
		 */
		virtual void close() throw( cms::CMSException );
		
		/**
		 * Commits the current transaction.
		 * Unsupported by this class - throws exception.
		 */
		virtual void commit() throw( cms::CMSException ){
			throw ActiveMQException( "operation (commit): unsupported by ActiveMQSession" );
		}
		
		/**
		 * Cancels the current transaction and rolls the system state
		 * back to before the transaction occurred.
		 * Unsupported by this class - throws exception.
		 */
		virtual void rollback() throw( cms::CMSException ){
			throw ActiveMQException( "operation (rollback): unsupported by ActiveMQSession" );
		}
		
		/**
		 * Indicates whether the session is in transacted mode.
		 * @return true if the session is in transacted mode.
		 */
		virtual bool getTransacted() const throw( cms::CMSException ){
			return false;
		}
		
		/**
		 * Creates a TextMessage object.
		 * @return a new text message object.
		 */
		virtual cms::TextMessage* createTextMessage() throw( cms::CMSException ); 
		
		/**
		 * Creates an initialized TextMessage object.
		 * @param msg The buffer to be set in the message.
		 * @return A new text message object.
		 */
		virtual cms::TextMessage* createTextMessage( const char* msg ) 
			throw( cms::CMSException );
			
		/**
		 * Creates a BytesMessage object.
		 * @return A new byte message object.
		 */
		virtual cms::BytesMessage* createBytesMessage() throw( cms::CMSException );
		
		/**
		 * Creates a publisher for the specified topic.
		 * @param topic The Topic to publish to, or null if this is an 
		 * unidentified producer
		 */
		virtual cms::TopicPublisher* createPublisher( const cms::Topic* topic ) 
			throw( cms::CMSException );
		
		/**
		 * Creates a subscriber to the specified topic.
		 * @param topic The Topic to publish to, or null if this is an 
		 * unidentified producer
		 */
		virtual cms::TopicSubscriber* createSubscriber( const cms::Topic* topic ) 
			throw( cms::CMSException );
										
		/**
		 * Creates a topic identity given a Topic name.
		 */				  
		virtual cms::Topic* createTopic( const char* topicName ) 
			throw( cms::CMSException );
			
		virtual ActiveMQConnection* getConnection(){
			return connection;
		}
		
		virtual const ActiveMQConnection* getConnection() const{
			return connection;		
		}
		
	private:
	
		ActiveMQConnection* connection;
		bool transacted;
		cms::Session::AcknowledgeMode acknowledgeMode;
	};
	
}

#endif /*ACTIVEMQ_ACTIVEMQSESSION_H_*/
