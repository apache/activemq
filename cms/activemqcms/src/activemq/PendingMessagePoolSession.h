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

#ifndef ACTIVEMQ_PENDINGMESSAGEPOOLSESSION_H_
#define ACTIVEMQ_PENDINGMESSAGEPOOLSESSION_H_

#include <activemq/PendingMessagePool.h>

namespace activemq{
	
	/**
	 * This class represents a single session for a given
	 * <code>PendingMessagePool</code> object. This class automates
	 * the session management to simplify the client code.
	 * 
	 * @author Nathan Mittler
	 */
	class PendingMessagePoolSession{
	public:
	
		/**
		 * Constructor - adds a session to the pool.
		 */
		PendingMessagePoolSession( PendingMessagePool* pool ){
			this->pool = pool;
			sessionId = pool->addSession();
		}
		
		/**
		 * Destructor - removes this session from the pool.
		 */
		virtual ~PendingMessagePoolSession(){
			pool->removeSession( sessionId );
		}
		
		/**
		 * Reads the next message for this session.  This is a destructive
		 * read.
		 */
		cms::Message* popNextPendingMessage(){
			
			return pool->popNextPendingMessage( sessionId );
		}
		
	private:
	
		/**
		 * The message pool.
		 */
		PendingMessagePool* pool;
		
		/**
		 * The unique identifier for this session.
		 */
		unsigned int sessionId;
	};
	
}

#endif /*ACTIVEMQ_PENDINGMESSAGEPOOLSESSION_H_*/
