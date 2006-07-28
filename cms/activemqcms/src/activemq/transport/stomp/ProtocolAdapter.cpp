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

#include "ProtocolAdapter.h"

using namespace activemq::transport::stomp;

////////////////////////////////////////////////////////////////////////////////
StompFrame::Command ProtocolAdapter::getCommandId( const StompMessage::MessageType type ){
	
	switch( type ){
		case StompMessage::MSG_CONNECT: return StompFrame::COMMAND_CONNECT;
		case StompMessage::MSG_CONNECTED: return StompFrame::COMMAND_CONNECTED;
		case StompMessage::MSG_DISCONNECT: return StompFrame::COMMAND_DISCONNECT;
		case StompMessage::MSG_SUBSCRIBE: return StompFrame::COMMAND_SUBSCRIBE;
		case StompMessage::MSG_UNSUBSCRIBE: return StompFrame::COMMAND_UNSUBSCRIBE;
		case StompMessage::MSG_TEXT: return StompFrame::COMMAND_SEND;
		case StompMessage::MSG_BYTES: return StompFrame::COMMAND_SEND;
		case StompMessage::MSG_BEGIN: return StompFrame::COMMAND_BEGIN;
		case StompMessage::MSG_COMMIT: return StompFrame::COMMAND_COMMIT;
		case StompMessage::MSG_ABORT: return StompFrame::COMMAND_ABORT;
		case StompMessage::MSG_ACK: return StompFrame::COMMAND_ACK;
		case StompMessage::MSG_ERROR: return StompFrame::COMMAND_ERROR;
		default: return StompFrame::NUM_COMMANDS;
	}
}

////////////////////////////////////////////////////////////////////////////////
StompMessage::MessageType ProtocolAdapter::getMessageType( const StompFrame* frame ){
	
	switch( frame->getCommand() ){
		case StompFrame::COMMAND_CONNECT: return StompMessage::MSG_CONNECT;
		case StompFrame::COMMAND_CONNECTED: return StompMessage::MSG_CONNECTED;
		case StompFrame::COMMAND_DISCONNECT: return StompMessage::MSG_DISCONNECT;
		case StompFrame::COMMAND_SUBSCRIBE: return StompMessage::MSG_SUBSCRIBE;
		case StompFrame::COMMAND_UNSUBSCRIBE: return StompMessage::MSG_UNSUBSCRIBE;
		case StompFrame::COMMAND_MESSAGE:{
			const StompFrame::HeaderInfo* info = frame->getHeaderInfo( StompFrame::HEADER_CONTENTLENGTH );
			if( info == NULL ){
				return StompMessage::MSG_TEXT;
			}
			return StompMessage::MSG_BYTES;
		}
		case StompFrame::COMMAND_BEGIN: return StompMessage::MSG_BEGIN; 
		case StompFrame::COMMAND_COMMIT: return StompMessage::MSG_COMMIT;
		case StompFrame::COMMAND_ABORT: return StompMessage::MSG_ABORT;
		case StompFrame::COMMAND_ACK: return StompMessage::MSG_ACK;
		case StompFrame::COMMAND_ERROR: return StompMessage::MSG_ERROR;
		default: return StompMessage::NUM_MSG_TYPES;
	}	
}

////////////////////////////////////////////////////////////////////////////////
cms::Session::AcknowledgeMode ProtocolAdapter::getAckMode( const StompFrame* frame ){
	
	const StompFrame::HeaderInfo* mode = frame->getHeaderInfo( StompFrame::HEADER_ACK );
	if( mode != NULL ){
	
		if( StompFrame::toAckMode( mode->value ) == StompFrame::ACK_CLIENT ){
			return cms::Session::CLIENT_ACKNOWLEDGE;
		}
	}
	
	return cms::Session::AUTO_ACKNOWLEDGE;
}

////////////////////////////////////////////////////////////////////////////////
const char* ProtocolAdapter::getAckModeString( const cms::Session::AcknowledgeMode mode ){
	
	if( mode == cms::Session::CLIENT_ACKNOWLEDGE ){
		return StompFrame::toString( StompFrame::ACK_CLIENT );
	}
	
	return StompFrame::toString( StompFrame::ACK_AUTO );
}

////////////////////////////////////////////////////////////////////////////////
int ProtocolAdapter::getAckModeStringLength( const cms::Session::AcknowledgeMode mode ){
	
	if( mode == cms::Session::CLIENT_ACKNOWLEDGE ){
		return StompFrame::getAckModeLength( StompFrame::ACK_CLIENT );
	}
	
	return StompFrame::getAckModeLength( StompFrame::ACK_AUTO );
}



