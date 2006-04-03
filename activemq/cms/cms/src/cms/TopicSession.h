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

#ifndef _CMS_TOPICSESSION_H_
#define _CMS_TOPICSESSION_H_
 
#include <cms/Session.h>
#include <string>

namespace cms{
	
	// Forward declarations.
	class Topic;
	class TopicPublisher;
	class TopicSubscriber;
	
	/**
	 * A TopicSession object provides methods for creating TopicPublisher, 
	 * TopicSubscriber, and Topic objects.
	 */
	class TopicSession : public Session{
		
	public:
	
		virtual ~TopicSession(){}
		
		/**
		 * Creates a publisher for the specified topic.
		 * @param topic The Topic to publish to, or null if this is an 
		 * unidentified producer
		 */
		virtual TopicPublisher* createPublisher( const Topic* topic ) 
			throw( CMSException ) = 0;
		
		/**
		 * Creates a subscriber to the specified topic.
		 * @param topic The Topic to publish to, or null if this is an 
		 * unidentified producer
		 */
		virtual TopicSubscriber* createSubscriber( const Topic* topic ) 
			throw( CMSException ) = 0;
										
		/**
		 * Creates a topic identity given a Topic name.
		 */				  
		virtual Topic* createTopic( const char* topicName ) 
			throw( CMSException ) = 0;
		
	};
}

#endif /*_CMS_TOPICSESSION_H_*/
