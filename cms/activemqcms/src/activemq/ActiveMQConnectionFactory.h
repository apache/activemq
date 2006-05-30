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

#ifndef ACTIVEMQ_ACTIVEMQCONNECTIONFACTORY_H_
#define ACTIVEMQ_ACTIVEMQCONNECTIONFACTORY_H_

#include <cms/TopicConnectionFactory.h>
#include <activemq/transport/TransportFactory.h>
#include <string>

namespace activemq{
	
	/**
	 * This is the implementation of the connection factory interfaces for
	 * use with the ActiveMQ stomp implementation.
	 * @author Nathan Mittler
	 */
	class ActiveMQConnectionFactory : public cms::TopicConnectionFactory
	{		
	public:
	
		/**
		 * Default constructor.
		 */
		ActiveMQConnectionFactory();
		
		/**
		 * Constructor for connecting to the broker with a default login.
		 * @param brokerUrl The URL of the stomp broker.
		 */
		ActiveMQConnectionFactory( const char* brokerUrl ) throw( cms::CMSException );
		
		/**
		 * Constructor with full argument list.
		 * @param userName The user name for connecting to the stomp broker.
		 * @param password The password for connecting to the stomp broker.
		 * @param brokerUrl The url of the stomp broker.
		 */
		ActiveMQConnectionFactory( const char* userName, const char* password, 
			const char* brokerUrl ) throw( cms::CMSException );
			
		/**
		 * Destructor.
		 */
		virtual ~ActiveMQConnectionFactory();
		
		/**
		 * Creates a topic connection with the default user identity.
		 */
		virtual cms::Connection* createConnection() throw( cms::CMSException );
		
		/**
		 * Creates a topic connection with the specified user identity.
		 */
		virtual cms::Connection* createConnection( 
			const char* userName, 
			const char* password ) throw( cms::CMSException );
			
		/**
		 * Creates a topic connection with the default user identity.
		 */
		virtual cms::TopicConnection* createTopicConnection() throw( cms::CMSException );
		
		/**
		 * Creates a topic connection with the specified user identity.
		 */
		virtual cms::TopicConnection* createTopicConnection( 
			const char* userName, 
			const char* password ) throw( cms::CMSException );
	
	private:
	
        /**
         * The url of the broker.
         */
        std::string brokerUrl;
		
		/**
		 * The user name for connecting to the broker.
		 */
		std::string userName;
		
		/**
		 * The password for connecting to the broker.
		 */
		std::string password;
        
        /**
         * Factory for transport objects.
         */
        transport::TransportFactory* transportFactory;
	};
	
}

#endif /*ACTIVEMQ_ACTIVEMQCONNECTIONFACTORY_H_*/
