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

#ifndef _CMS_TOPICCONNECTIONFACTORY_H_
#define _CMS_TOPICCONNECTIONFACTORY_H_
 
#include <cms/CMSException.h>
#include <cms/ConnectionFactory.h>

namespace cms{
	
	// Forward declarations.
	class TopicConnection;
	
	/**
	 * A ConnectionFactory object encapsulates a set of connection 
	 * configuration parameters that has been defined by an administrator. 
	 * A client uses it to create a connection with a STOMP provider.
	 */
	class TopicConnectionFactory : public ConnectionFactory{
		
	public:
	
		virtual ~TopicConnectionFactory(){}
		
		/**
		 * Creates a topic connection with the default user identity.
		 */
		virtual TopicConnection* createTopicConnection() throw( CMSException ) = 0;
		
		/**
		 * Creates a topic connection with the specified user identity.
		 */
		virtual TopicConnection* createTopicConnection( 
			const char* userName, 
			const char* password ) throw( CMSException ) = 0;
	};
}

#endif /*_CMS_TOPICCONNECTIONFACTORY_H_*/
