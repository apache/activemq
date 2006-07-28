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

#include "ActiveMQPublisher.h"
#include "ActiveMQTopic.h"
#include "ActiveMQSession.h"
#include "ActiveMQConnection.h"
#include "transport/Transport.h"

using namespace activemq;
using namespace activemq::transport;
using namespace cms;

////////////////////////////////////////////////////////////////////////////////
ActiveMQPublisher::ActiveMQPublisher( const Topic* topic, 
	ActiveMQSession* session )
{
	this->topic = topic;
	this->session = session;
}

////////////////////////////////////////////////////////////////////////////////
ActiveMQPublisher::~ActiveMQPublisher()
{
}

////////////////////////////////////////////////////////////////////////////////
void ActiveMQPublisher::publish( Message* message ) 
	throw( CMSException ){
	
	publish( topic, message );
}

////////////////////////////////////////////////////////////////////////////////
void ActiveMQPublisher::publish( const Topic* topic, 
	Message* message ) throw( CMSException )
{    
    
    if( topic == NULL ){        
        throw ActiveMQException( "ActiveMQPublisher::publish() - invalid topic" );
    }
    
    if( message == NULL ){
        throw ActiveMQException( "ActiveMQPublisher::publish() - invalid message" );
    }
    
    // Get the transport object.
    Transport* transport = session->getConnection()->getTransportChannel();
    if( transport == NULL ){
        throw ActiveMQException( "ActiveMQPublisher::publish() - invalid transport layer" );
    }
    
    // Send the message.
    transport->sendMessage( topic, message );
}

