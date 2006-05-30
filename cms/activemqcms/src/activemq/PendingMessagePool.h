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

#ifndef ACTIVEMQ_PENDINGMESSAGEPOOL_H_
#define ACTIVEMQ_PENDINGMESSAGEPOOL_H_

#include <cms/MessageListener.h>
#include <activemq/concurrent/Mutex.h>
#include <vector>
#include <list>
#include <map>

namespace activemq{
	
	/**
	 * Represents a pool of pending messages that have yet to be
	 * read by message consumers.  Each consumer is given a session
	 * into this pool.  When a new message comes in, a copy of it is given to
	 * each session.
	 * 
	 * @author Nathan Mittler
	 */
	class PendingMessagePool : public cms::MessageListener
	{
	public:
	
		/**
		 * Default constructor.
		 */
		PendingMessagePool();
		
		/**
		 * Destructor - destroys any remaining session queues.
		 */
		virtual ~PendingMessagePool();
		
		/**
		 * Adds a new session.
		 * @return 	The id of the added session.
		 */
		virtual unsigned int addSession();
		
		/**
		 * Removes a session and destroys any pending messages for
		 * that session.
		 * @param sessionId The id of the session to be removed.
		 */
		virtual void removeSession( const unsigned int sessionId );
		
		/**
		 * Reads the next message on the queue for the given session.  This is
		 * a destructive read - the message will be popped from the queue.
		 * @param sessionId The id of the session.
		 * @return The next message for the session.  NULL if no messages exist.
		 */
		virtual cms::Message* popNextPendingMessage(
			const unsigned int sessionId );
			
		/**
		 * Invoked to add a new message to the pool
		 * @param msg The new message to be added.
		 */
		virtual void onMessage( const cms::Message* msg );
		
	private:
	
		/**
		 * The vector of session queues.  Each session is given a std::pair where
		 * the first is a flag where true means the session is active.  The second
		 * is the queue of messages for that session.  
		 */
		std::vector< std::pair< bool, std::list<cms::Message*> > > sessionQueues;
		
		/**
		 * Synchronization mechanism.
		 */
		concurrent::Mutex mutex;
	};

}

#endif /*ACTIVEMQ_PENDINGMESSAGEPOOL_H_*/
