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

#include "PendingMessagePool.h"
#include "concurrent/Lock.h"
#include <cms/Message.h>

using namespace activemq;
using namespace activemq::concurrent;
using namespace std;

////////////////////////////////////////////////////////////////////////////////
PendingMessagePool::PendingMessagePool()
{
}

////////////////////////////////////////////////////////////////////////////////
PendingMessagePool::~PendingMessagePool()
{
	// Remove all sessions.
	for( unsigned int ix=0; ix<sessionQueues.size(); ++ix ){
		removeSession(ix);
	}
}

////////////////////////////////////////////////////////////////////////////////
unsigned int PendingMessagePool::addSession(){
	
	// Lock this class.
	Lock lock( &mutex );
	
	// First, check any of the existing queues to see if they're no longer
	// active (the boolean first in the pair indicates whether or not the
	// given queue is actively being used).  If an inactive queue is found,
	// use it.
	for( unsigned int ix=0; ix<sessionQueues.size(); ++ix ){
		if( sessionQueues[ix].first == false ){
			sessionQueues[ix].first = true;
			sessionQueues[ix].second.clear();
			return ix;
		}
	}
	
	// No existing inactive queue was found - add one for this session.
	sessionQueues.push_back( pair<bool, list<cms::Message*> >() );
	
	// Get the position of the inserted element.
	unsigned int pos = sessionQueues.size()-1;
	
	// Mark this session as active.
	sessionQueues[pos].first = true;
	
	// Return the index of the last added element as the session id.
	return pos;
}

////////////////////////////////////////////////////////////////////////////////
void PendingMessagePool::removeSession( const unsigned int sessionId ){
	
	// Lock this class.
	Lock lock( &mutex );
	
	// If the session is not in the pool, just return.
	if( sessionId >= sessionQueues.size() ){
		return;
	}
	
	// Mark this session as inactive.
	sessionQueues[sessionId].first = false;
	
	list<cms::Message*>& msgs = sessionQueues[sessionId].second;
	list<cms::Message*>::iterator iter = msgs.begin();
	for( ; iter != msgs.end(); ++iter ){
        cms::Message* msg = *iter;
        delete msg;
	}
	msgs.clear();
}

////////////////////////////////////////////////////////////////////////////////
cms::Message* PendingMessagePool::popNextPendingMessage(
	const unsigned int sessionId ){
	
	// Lock this class.
	Lock lock( &mutex );
	
	// If this is an invalid or inactive session, just return NULL.
	if( sessionId > sessionQueues.size() || sessionQueues[sessionId].first == false ){
		return NULL;
	}
	
	// Get the message q for this session.
	list<cms::Message*>& msgs = sessionQueues[sessionId].second;
	
	// Get the number of messages in the queue.
	unsigned int numMessages = msgs.size();
	
	// If there are no messages, just return.
	if( numMessages == 0 ){
		return NULL;
	}
	
	// Remove the first message from the queue.
	cms::Message* nextMsg = msgs.front();
	msgs.pop_front();
	
	// Return the next message to the caller.  It is now the
	// responsibility of the caller to free this memory.
	return nextMsg;
}

////////////////////////////////////////////////////////////////////////////////
void PendingMessagePool::onMessage( const cms::Message* msg ){
	
	// Lock this class.
	Lock lock( &mutex );
	
	// Add a copy of this message to each active session queue.
	for( unsigned int ix=0; ix<sessionQueues.size(); ++ix ){
		
		if( sessionQueues[ix].first == true ){
			
			list<cms::Message*>& msgs = sessionQueues[ix].second;
			
            // Cast away const-ness
            cms::Message* tempMsg = const_cast<cms::Message*>(msg);
            
			// Clone the message.
			msgs.push_back( tempMsg->clone() );
		}
	}
}

