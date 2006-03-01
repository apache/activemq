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

#include "ActiveMQConnectionFactory.h"
#include "ActiveMQConnection.h"
#include "transport/stomp/StompTransportFactory.h"
#include <stdio.h>
#include <sstream>

using namespace activemq;
using namespace activemq::transport;
using namespace cms;
using namespace std;

////////////////////////////////////////////////////////////////////////////////
ActiveMQConnectionFactory::ActiveMQConnectionFactory()
{	
    transportFactory = new activemq::transport::stomp::StompTransportFactory();
}

////////////////////////////////////////////////////////////////////////////////
ActiveMQConnectionFactory::ActiveMQConnectionFactory( 
	const char* brokerUrl ) throw( CMSException )
{
    this->brokerUrl = brokerUrl;
    transportFactory = new activemq::transport::stomp::StompTransportFactory();		
}

////////////////////////////////////////////////////////////////////////////////
ActiveMQConnectionFactory::ActiveMQConnectionFactory( const char* userName, 
	const char* password, 
	const char* brokerUrl ) throw( CMSException )
{
	this->userName = userName;
	this->password = password;

    brokerUrl = brokerUrl;
    
    transportFactory = new activemq::transport::stomp::StompTransportFactory();
}
			
////////////////////////////////////////////////////////////////////////////////
ActiveMQConnectionFactory::~ActiveMQConnectionFactory()
{
    if( transportFactory != NULL ){
        delete transportFactory;
    }
}

////////////////////////////////////////////////////////////////////////////////
Connection* ActiveMQConnectionFactory::createConnection() throw( CMSException ){
	return createTopicConnection();
}

////////////////////////////////////////////////////////////////////////////////
Connection* ActiveMQConnectionFactory::createConnection( 
	const char* userName, 
	const char* password ) throw( CMSException )
{		
	return createTopicConnection( userName, password );
}

////////////////////////////////////////////////////////////////////////////////
TopicConnection* ActiveMQConnectionFactory::createTopicConnection()
	throw( CMSException )
{	
    Transport* transport = transportFactory->createTransport( brokerUrl.c_str() );
    
    return new ActiveMQConnection( transport );
}

////////////////////////////////////////////////////////////////////////////////
TopicConnection* ActiveMQConnectionFactory::createTopicConnection( 
	const char* userName, 
	const char* password ) throw( CMSException )
{        
    Transport* transport = transportFactory->createTransport( brokerUrl.c_str(),
        userName, 
        password );
    
    return new ActiveMQConnection( transport );
}


