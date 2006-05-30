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

#include "ActiveMQConnection.h"
#include "ActiveMQException.h"
#include "ActiveMQSession.h"
#include "transport/Transport.h"
#include <cms/ExceptionListener.h>

using namespace activemq;
using namespace activemq::transport;
using namespace cms;
using namespace std;

////////////////////////////////////////////////////////////////////////////////
ActiveMQConnection::ActiveMQConnection( Transport* transport )
{
	this->transport = transport;
	this->transport->setExceptionListener( this );
    this->exceptionListener = NULL;
}

////////////////////////////////////////////////////////////////////////////////
ActiveMQConnection::~ActiveMQConnection()
{
	close();
    
    if( transport != NULL ){
        transport->setExceptionListener( NULL );
        delete transport;
    }
}

////////////////////////////////////////////////////////////////////////////////
void ActiveMQConnection::start() throw( CMSException ){
        
    if( transport != NULL ){
        
        // Connect if necessary and start the flow of messages.
        transport->start();    
    }
}

////////////////////////////////////////////////////////////////////////////////
void ActiveMQConnection::stop() throw( CMSException ){

    if( transport != NULL ){
        
        // Stop the flow of messages.
    	transport->stop();
    }
}

////////////////////////////////////////////////////////////////////////////////
void ActiveMQConnection::close() throw( CMSException )
{
	if( transport != NULL ){
        
        // Disconnect from the broker
        transport->close();
	}
}

////////////////////////////////////////////////////////////////////////////////	
TopicSession* ActiveMQConnection::createTopicSession( 
	const bool transacted, 
	const Session::AcknowledgeMode acknowledgeMode ) 
	throw( CMSException )
{	
	if( !transacted ){
			
		ActiveMQSession* session = new ActiveMQSession( this, acknowledgeMode );
		return session;
	}
	
	return NULL;
}

////////////////////////////////////////////////////////////////////////////////
void ActiveMQConnection::onException( const CMSException* exception ){
	
	if( exceptionListener ){
		exceptionListener->onException( exception );
	}
}




