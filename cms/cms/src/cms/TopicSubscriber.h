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

#ifndef _CMS_TOPICSUBSCRIBER_H_
#define _CMS_TOPICSUBSCRIBER_H_
 
#include <cms/MessageConsumer.h>

namespace cms{
	
	// Forward declarations.
	class Topic;
	
	/**
	 * A client uses a TopicSubscriber object to receive messages 
	 * that have been published to a topic. A TopicSubscriber object 
	 * is the publish/subscribe form of a message consumer.
	 */
	class TopicSubscriber : public MessageConsumer{
		
	public:
	
		virtual ~TopicSubscriber(){}
		
		/**
		 * Gets the Topic associated with this subscriber.
		 */
		virtual const Topic* getTopic() const throw( CMSException ) = 0;
	};
}

#endif /*_CMS_TOPICSUBSCRIBER_H_*/
