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

#include "DestinationPool.h"

using namespace activemq::transport::stomp;
using namespace cms;
using namespace std;

////////////////////////////////////////////////////////////////////////////////
DestinationPool::DestinationPool()
{
}

////////////////////////////////////////////////////////////////////////////////
DestinationPool::~DestinationPool()
{
}

////////////////////////////////////////////////////////////////////////////////
void DestinationPool::addListener( const string& destination, 
	MessageListener* listener )
{
	vector<MessageListener*>& listeners = listenerMap[destination];
	for( unsigned int ix=0; ix<listeners.size(); ++ix ){
		if( listeners[ix] == listener ){
			return;
		}
	}
	
	listeners.push_back( listener );
}

////////////////////////////////////////////////////////////////////////////////
void DestinationPool::removeListener( const string& destination, 
	MessageListener* listener )
{
	// Locate the listeners vector.
	map< string, vector<MessageListener*> >::iterator iter = 
		listenerMap.find( destination );
		
	// If no entry for this destination exists - just return.
	if( iter == listenerMap.end() ){
		return;
	}
	
	vector<MessageListener*>& listeners = iter->second;
	vector<MessageListener*>::iterator listenerIter = listeners.begin();
	for( ; listenerIter != listeners.end(); ++listenerIter ){
		if( *listenerIter == listener ){
			listeners.erase( listenerIter );
			return;
		}
	}
	
	// If there are no more listeners of this destination - remove
	// the listeners list from the map.
	if( listeners.size() == 0 ){
		listenerMap.erase( destination );
	}
}

////////////////////////////////////////////////////////////////////////////////
bool DestinationPool::hasListeners( const string& destination ){
	
	std::map< std::string, std::vector<MessageListener*> >::iterator iter = 
		listenerMap.find( destination );
	if( iter == listenerMap.end() ){
		return false;
	}
	
	vector<MessageListener*>& listeners = listenerMap[destination];
	return listeners.size() != 0;
}

////////////////////////////////////////////////////////////////////////////////
void DestinationPool::notify( const std::string& destination, 
    const Message* msg ){
	
	// Locate the listeners vector.
	std::map< std::string, std::vector<MessageListener*> >::iterator iter = 
		listenerMap.find( destination );
		
	// If no entry for this destination exists - just return.
	if( iter == listenerMap.end() ){
		return;
	}
		
	// Create a copy of the vector.  This will allow the listeners to
	// unregister in their callbacks, without corrupting this iteration.
	vector<MessageListener*> listeners = iter->second;
	for( unsigned int ix=0; ix<listeners.size(); ++ix ){
        MessageListener* listener = listeners[ix];
		listener->onMessage( msg );
	}
}


