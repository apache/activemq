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

#ifndef ACTIVEMQ_ACTIVEMQPUBLISHER_H_
#define ACTIVEMQ_ACTIVEMQPUBLISHER_H_

#include <cms/TopicPublisher.h>
#include <cms/Topic.h>

namespace activemq{
	
	// Forward declarations.
	class ActiveMQSession;
	
    /**
     * Basic implementation of the topic publisher interface.
     * @author Nathan Mittler
     */
	class ActiveMQPublisher : public cms::TopicPublisher
	{
	public:
		ActiveMQPublisher( const cms::Topic* topic, 
			ActiveMQSession* session );
		virtual ~ActiveMQPublisher();
		
		/**
		 * Gets the topic associated with this TopicPublisher.
		 */
		virtual const cms::Topic* getTopic() const throw( cms::CMSException ){
			return topic;
		}
		
		/**
		 * Publishes a message to the topic.
		 * @param message the message to publish
		 */
		virtual void publish( cms::Message* message ) 
			throw( cms::CMSException );
		
		/**
		 * Publishes a message to a topic for an unidentified message producer.
		 * @param topic The topic to publish this message to.
		 * @param message The message to publish
		 */
		virtual void publish( const cms::Topic* topic, 
			cms::Message* message ) throw( cms::CMSException );
						
	private:
	
		const cms::Topic* topic;
		ActiveMQSession* session;
	};
	
}

#endif /*ACTIVEMQ_ACTIVEMQPUBLISHER_H_*/
