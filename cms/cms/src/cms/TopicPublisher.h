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

#ifndef _CMS_TOPICPUBLISHER_H_
#define _CMS_TOPICPUBLISHER_H_
 
#include <cms/MessageProducer.h>
#include <cms/CMSException.h>

namespace cms{
	
	// Forward declarations
	class Message;
	class Topic;
	
	/**
	 * A client uses a TopicPublisher object to publish messages on 
	 * a topic. A TopicPublisher object is the publish-subscribe form 
	 * of a message producer.
	 */
	class TopicPublisher : public MessageProducer{
		
	public:
	
		virtual ~TopicPublisher(){}
		
		/**
		 * Gets the topic associated with this TopicPublisher.
		 */
		virtual const Topic* getTopic() const throw( CMSException ) = 0;
		
		/**
		 * Publishes a message to the topic.
		 * @param message the message to publish
		 */
		virtual void publish( Message* message ) 
			throw( CMSException ) = 0;
		
		/**
		 * Publishes a message to a topic for an unidentified message producer.
		 * @param topic The topic to publish this message to.
		 * @param message The message to publish
		 */
		virtual void publish( const Topic* topic, 
			Message* message ) throw( CMSException ) = 0;
	};
}

#endif /*_CMS_TOPICPUBLISHER_H_*/
