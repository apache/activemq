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
 
#include "StompFrame.h"
#include <stdio.h>

using namespace activemq::transport::stomp;
using namespace std;

bool StompFrame::staticInitialized = false;
const char* StompFrame::standardHeaders[NUM_STANDARD_HEADERS];
int StompFrame::standardHeaderLengths[NUM_STANDARD_HEADERS];	
const char* StompFrame::commands[NUM_COMMANDS];
int StompFrame::commandLengths[NUM_COMMANDS];
const char* StompFrame::ackModes[NUM_ACK_MODES];
int StompFrame::ackModeLengths[NUM_ACK_MODES];

////////////////////////////////////////////////////////////////////////////////
StompFrame::StompFrame(){

	body = NULL;
	bodyLength = 0;
	
	if( !staticInitialized ){
		staticInit();
	}
}

////////////////////////////////////////////////////////////////////////////////
StompFrame::~StompFrame(){
}

void StompFrame::staticInit(){
	
	standardHeaders[HEADER_DESTINATION] = "destination";
	standardHeaders[HEADER_TRANSACTIONID] = "transaction";
	standardHeaders[HEADER_CONTENTLENGTH] = "content-length";
	standardHeaders[HEADER_SESSIONID] = "session";
	standardHeaders[HEADER_RECEIPTID] ="receipt";
	standardHeaders[HEADER_MESSAGEID] = "message-id";
	standardHeaders[HEADER_ACK] = "ack";
	standardHeaders[HEADER_LOGIN] = "login";
	standardHeaders[HEADER_PASSWORD] = "passcode";
	standardHeaders[HEADER_MESSAGE] = "message";
	commands[COMMAND_CONNECT] = "CONNECT";
	commands[COMMAND_CONNECTED] = "CONNECTED";
	commands[COMMAND_DISCONNECT] = "DISCONNECT";
	commands[COMMAND_SUBSCRIBE] = "SUBSCRIBE";
	commands[COMMAND_UNSUBSCRIBE] = "UNSUBSCRIBE";
	commands[COMMAND_MESSAGE] = "MESSAGE";
	commands[COMMAND_SEND] = "SEND";
	commands[COMMAND_BEGIN] = "BEGIN";
	commands[COMMAND_COMMIT] = "COMMIT";
	commands[COMMAND_ABORT] = "ABORT";
	commands[COMMAND_ACK] = "ACK";
	commands[COMMAND_ERROR] = "ERROR";
	ackModes[ACK_CLIENT] = "client";
	ackModes[ACK_AUTO] = "auto";
	
	// Assign all the string lengths for the standard headers.
	for( int ix=0; ix<NUM_STANDARD_HEADERS; ++ix ){
		standardHeaderLengths[ix] = strlen(standardHeaders[ix]);
	}
	
	// Assign all the string lengths for the commands.
	for( int ix=0; ix<NUM_COMMANDS; ++ix ){
		commandLengths[ix] = strlen(commands[ix]);
	}
	
	// Assign all the string lengths for the ack modes.
	for( int ix=0; ix<NUM_ACK_MODES; ++ix ){
		ackModeLengths[ix] = strlen(ackModes[ix]);
	}

	staticInitialized = true;
}

////////////////////////////////////////////////////////////////////////////////
void StompFrame::setHeader( const char* key, const int keyLength,
	const char* value, 
	const int valueLength  )
{	
	HeaderInfo info;
	info.key = key;
	info.keyLength = keyLength;
	info.value = value;
	info.valueLength = valueLength;
	
	headers[key] = info;
}

////////////////////////////////////////////////////////////////////////////////
const StompFrame::HeaderInfo* StompFrame::getHeaderInfo( const char* name ) const{
	
	map< string, HeaderInfo >::const_iterator pos = headers.find( name );
	if( pos == headers.end() ){
		return NULL;
	}
	
	return &(pos->second);
}

////////////////////////////////////////////////////////////////////////////////
void StompFrame::setBodyText( const char* text, const int length ){
	
	body = text;
	bodyLength = length;
}

////////////////////////////////////////////////////////////////////////////////
void StompFrame::setBodyBytes( const char* bytes, int numBytes ){
		
	body = bytes;
	bodyLength = numBytes;

	// Set the content-length header.
	sprintf( bodyLengthStr, "%d", numBytes );
	setHeader( standardHeaders[HEADER_CONTENTLENGTH], 
		standardHeaderLengths[HEADER_CONTENTLENGTH],
		bodyLengthStr,
		strlen(bodyLengthStr) );
}


