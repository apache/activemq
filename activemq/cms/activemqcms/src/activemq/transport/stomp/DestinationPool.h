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

#ifndef ACTIVEMQ_TRANSPORT_STOMP_DESTINATIONPOOL_H_
#define ACTIVEMQ_TRANSPORT_STOMP_DESTINATIONPOOL_H_

#include <cms/Message.h>
#include <cms/MessageListener.h>
#include <map>
#include <vector>
#include <string>

namespace activemq{
namespace transport{
namespace stomp{
	
    /**
     * Maps destination (topic) names to the subscribers
     * of that topic.
     * @author Nathan Mittler
     */
	class DestinationPool
	{
	public:
    
		DestinationPool();
		virtual ~DestinationPool();
		
		void addListener( const std::string& destination, cms::MessageListener* listener );
		void removeListener( const std::string& destination, cms::MessageListener* listener );
		bool hasListeners( const std::string& destination );
		void notify( const std::string& destination, const cms::Message* msg );
		
	private:
	
		std::map< std::string, std::vector<cms::MessageListener*> > listenerMap;
	};

}}}

#endif /*ACTIVEMQ_TRANSPORT_STOMP_DESTINATIONPOOL_H_*/
