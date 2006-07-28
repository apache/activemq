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

#include "ActiveMQSession.h"
#include "ActiveMQConnection.h"
#include "ActiveMQTopic.h"
#include "ActiveMQTextMessage.h"
#include "ActiveMQBytesMessage.h"
#include "ActiveMQPublisher.h"
#include "ActiveMQSubscriber.h"

using namespace activemq;
using namespace cms;
using namespace std;

////////////////////////////////////////////////////////////////////////////////
ActiveMQSession::ActiveMQSession( ActiveMQConnection* connection,
	const Session::AcknowledgeMode acknowledgeMode )
{
	this->connection = connection;
	this->transacted = transacted;
	this->acknowledgeMode = acknowledgeMode;
}

////////////////////////////////////////////////////////////////////////////////
ActiveMQSession::~ActiveMQSession()
{
	close();
}

////////////////////////////////////////////////////////////////////////////////
void ActiveMQSession::close() throw( CMSException ){
}

////////////////////////////////////////////////////////////////////////////////
TextMessage* ActiveMQSession::createTextMessage() throw( CMSException ){
	ActiveMQTextMessage* textMsg = new ActiveMQTextMessage();
	return textMsg;
}

////////////////////////////////////////////////////////////////////////////////	
TextMessage* ActiveMQSession::createTextMessage( const char* msg ) throw( CMSException )
{
	ActiveMQTextMessage* textMsg = new ActiveMQTextMessage();
	textMsg->setText( msg );
	return textMsg;
}

////////////////////////////////////////////////////////////////////////////////
BytesMessage* ActiveMQSession::createBytesMessage() throw( CMSException ){
	ActiveMQBytesMessage* bMsg = new ActiveMQBytesMessage();
	return bMsg;
}

////////////////////////////////////////////////////////////////////////////////
TopicPublisher* ActiveMQSession::createPublisher( const Topic* topic ) 
	throw( CMSException )
{
	return new ActiveMQPublisher( topic, this );
}

////////////////////////////////////////////////////////////////////////////////
TopicSubscriber* ActiveMQSession::createSubscriber( const Topic* topic ) 
	throw( CMSException )
{
	ActiveMQSubscriber* subscriber = new ActiveMQSubscriber( topic, this );	
	return subscriber;	
}

////////////////////////////////////////////////////////////////////////////////
Topic* ActiveMQSession::createTopic( const char* topicName ) 
	throw( CMSException )
{
	return new ActiveMQTopic( topicName );
}




			
			
