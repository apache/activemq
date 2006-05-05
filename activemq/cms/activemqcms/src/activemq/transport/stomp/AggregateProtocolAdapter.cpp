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

#include "AggregateProtocolAdapter.h"
#include "ConnectProtocolAdapter.h"
#include "ConnectedProtocolAdapter.h"
#include "DisconnectProtocolAdapter.h"
#include "SubscribeProtocolAdapter.h"
#include "UnsubscribeProtocolAdapter.h"
#include "TextProtocolAdapter.h"
#include "BytesProtocolAdapter.h"
#include "ErrorProtocolAdapter.h"

using namespace activemq::transport::stomp;

////////////////////////////////////////////////////////////////////////////////
AggregateProtocolAdapter::AggregateProtocolAdapter()
:
	adapters( StompMessage::NUM_MSG_TYPES )
{
	// Zero out all elements of the array.
	for( unsigned int ix=0; ix<adapters.size(); ++ix ){
		adapters[ix] = NULL;
	}
	
	adapters[StompMessage::MSG_CONNECT] = new ConnectProtocolAdapter();
	adapters[StompMessage::MSG_CONNECTED] = new ConnectedProtocolAdapter();
	adapters[StompMessage::MSG_DISCONNECT] = new DisconnectProtocolAdapter();
	adapters[StompMessage::MSG_SUBSCRIBE] = new SubscribeProtocolAdapter();
	adapters[StompMessage::MSG_UNSUBSCRIBE] = new UnsubscribeProtocolAdapter();
	adapters[StompMessage::MSG_TEXT] = new TextProtocolAdapter();
	adapters[StompMessage::MSG_BYTES] = new BytesProtocolAdapter();
	adapters[StompMessage::MSG_ERROR] = new ErrorProtocolAdapter();
}

////////////////////////////////////////////////////////////////////////////////
AggregateProtocolAdapter::~AggregateProtocolAdapter()
{
	for( unsigned int ix=0; ix<adapters.size(); ++ix ){
		if( adapters[ix] != NULL ){
			delete adapters[ix];
			adapters[ix] = NULL;
		}
	}
}

////////////////////////////////////////////////////////////////////////////////
StompMessage* AggregateProtocolAdapter::adapt( const StompFrame* frame ){
	
	StompMessage::MessageType msgType = getMessageType( frame );
	if( ((unsigned int)msgType) < adapters.size() && adapters[msgType] != NULL ){
		ProtocolAdapter* adapter = adapters[msgType];
		return adapter->adapt( frame );
	}
	
	return NULL;
}

////////////////////////////////////////////////////////////////////////////////
StompFrame* AggregateProtocolAdapter::adapt( const StompMessage* message ){
	
	StompMessage::MessageType msgType = message->getMessageType();
	if( ((unsigned int)msgType) < adapters.size() && adapters[msgType] != NULL ){
		ProtocolAdapter* adapter = adapters[msgType];
		return adapter->adapt( message );
	}
	
	return NULL;
}
