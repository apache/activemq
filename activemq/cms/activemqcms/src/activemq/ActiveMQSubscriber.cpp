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

#include "ActiveMQSubscriber.h"
#include "ActiveMQSession.h"
#include "PendingMessagePoolSession.h"
#include "ActiveMQConnection.h"
#include "transport/Transport.h"
#include <sstream>
#include <cms/MessageListener.h>
#include <time.h>
#include <sys/time.h>

using namespace activemq;
using namespace activemq::transport;
using namespace cms;
using namespace std;

////////////////////////////////////////////////////////////////////////////////
ActiveMQSubscriber::ActiveMQSubscriber( const cms::Topic* topic,
	ActiveMQSession* session )
{
	this->topic = topic;
	this->session = session;
        
    // Add this object as a listener to the topic.
    Transport* transport = session->getConnection()->getTransportChannel();
    if( transport != NULL ){
        transport->addMessageListener( topic, this );
    }
}

////////////////////////////////////////////////////////////////////////////////
ActiveMQSubscriber::~ActiveMQSubscriber()
{
}

////////////////////////////////////////////////////////////////////////////////
void ActiveMQSubscriber::onMessage( const cms::Message* message ){
	
	// Notify the message listener of the new message.
	notify( message );
	
	// Add the message to the pending message pool for any synchronous consumers
	// (e.g. calls to receive(),etc).
	pendingMessagePool.onMessage( message );
}

////////////////////////////////////////////////////////////////////////////////
void ActiveMQSubscriber::close() throw( cms::CMSException ){
        
    // Remove this object as a listener to the topic.
    Transport* transport = session->getConnection()->getTransportChannel();
    if( transport != NULL ){
        transport->removeMessageListener( topic, this );
    }
}

////////////////////////////////////////////////////////////////////////////////
void ActiveMQSubscriber::notify( const Message* msg ){
	
	if( messageListener != NULL ){
		messageListener->onMessage( msg );
	}
}

////////////////////////////////////////////////////////////////////////////////
Message* ActiveMQSubscriber::receive() throw( CMSException ){
	
	// Create a session for the pending message pool.
	PendingMessagePoolSession session( &pendingMessagePool );
	
	// Create the sleep time interval.  We'll sleep for a short
	// time between each attempted read.
	timespec sleepInterval;
	sleepInterval.tv_sec = readSleepMicroseconds / 1000000;
	sleepInterval.tv_nsec = (readSleepMicroseconds - (sleepInterval.tv_sec * 1000000 ))*1000;
		
	// Loop until we successfully receive the next message.
	while( true ){
	
		// Read the next message for this session.
		cms::Message* msg = session.popNextPendingMessage();
		if( msg != NULL ){
			
			// We successfully received the next message - return it.
			return msg;
		}
		
		// No message exists, sleep a short while then try again.
		timespec remaining;
		nanosleep(&sleepInterval, &remaining);	
	}
}

////////////////////////////////////////////////////////////////////////////////
Message* ActiveMQSubscriber::receive( long timeout ) throw( CMSException ){
	
	// Create a session for the pending message pool.
	PendingMessagePoolSession session( &pendingMessagePool );
	
	// Create the sleep time interval.  We'll sleep for a short
	// time between each attempted read.
	timespec sleepInterval;
	sleepInterval.tv_sec = readSleepMicroseconds / 1000000;
	sleepInterval.tv_nsec = (readSleepMicroseconds - (sleepInterval.tv_sec * 1000000 ))*1000;
	
	// Get the current time.
	timeval temp;
	gettimeofday( &temp, NULL );
	
	// Get the start time in microseconds.	
	long currTime = (temp.tv_sec * 1000000) + temp.tv_usec;
	
	// Determine the end time of this receive.  We will not wait
	// any longer than this time.
	long endTime = currTime + (timeout * 1000);
		
	// Loop while we haven't exceeded our max wait time.
	while( currTime < endTime ){
	
		// Read the next message for this session.
		cms::Message* msg = session.popNextPendingMessage();
		if( msg != NULL ){
			
			// We successfully received the next message - return it.
			return msg;
		}
		
		// No message exists, sleep a short while then try again.
		timespec remaining;
		nanosleep(&sleepInterval, &remaining);
			
		// Update the running time total.
		currTime += readSleepMicroseconds;			
	}
	
	return NULL;
}

////////////////////////////////////////////////////////////////////////////////
Message* ActiveMQSubscriber::receiveNoWait() throw( CMSException ){
	
	// Create a session for the pending message pool.
	PendingMessagePoolSession session( &pendingMessagePool );
	
	// Read the next message for this session and return it.
	cms::Message* msg = session.popNextPendingMessage();	
	return msg;
}

