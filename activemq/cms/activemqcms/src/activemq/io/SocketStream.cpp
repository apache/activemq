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
 
#include "SocketStream.h"
#include "IOException.h"
#include "Socket.h"
#include <sys/poll.h>
#include <sys/socket.h>
#include <errno.h>
#include <string.h>
#include <string>

using namespace activemq::io;
using namespace std;

extern int errno;

////////////////////////////////////////////////////////////////////////////////
SocketStream::SocketStream( Socket* socket )
{
	this->socket = socket;
}

////////////////////////////////////////////////////////////////////////////////
SocketStream::~SocketStream()
{
}

////////////////////////////////////////////////////////////////////////////////
int SocketStream::available() const{
	
	// Poll the socket for input.	
	pollfd fd;
	fd.fd = socket->getHandle();
	fd.events = POLLIN;
	fd.revents = POLLIN;
	int status = poll( &fd, 1, 1 );
	if( status > 0 ){
		return 1;
	}
	
	return 0;
}

////////////////////////////////////////////////////////////////////////////////
char SocketStream::read() throw (ActiveMQException){
	
	char c;
	
	int len = recv( socket->getHandle(), &c, sizeof(c), 0 );
	if( len != sizeof(c) ){
        socket->close();
		char buf[500];
		strerror_r( errno, buf, 500 );
		throw IOException( string("stomp::io::SocketStream::read() - ") + buf );
	}
	
	return c;
}

////////////////////////////////////////////////////////////////////////////////
int SocketStream::read( char* buffer, const int bufferSize ) throw (ActiveMQException){
	
	int len = recv( socket->getHandle(), buffer, bufferSize, 0 );
	if( len < 0 ){
        socket->close();
		char buf[500];
		strerror_r( errno, buf, 500 );
		throw IOException( string("stomp::io::SocketStream::read(char*,int) - ") + buf );
	}
	
    /*printf("SocketStream:read():");
    for( int ix=0; ix<len; ++ix ){
        if( buffer[ix] > 20 )
            printf("%c", buffer[ix] );
        else
            printf("[%d]", buffer[ix] );
    }
    printf("\n");*/
    
	return len;
}

////////////////////////////////////////////////////////////////////////////////
void SocketStream::write( const char c ) throw (ActiveMQException){
	
	/*if( c > 20 ){
		printf("%c", c );
	}
	else printf("[%d]", c );*/
	
	int success = send( socket->getHandle(), &c, sizeof(c), MSG_NOSIGNAL );
	if( success < 0 ){
        socket->close();
		char buf[500];
		strerror_r( errno, buf, 500 );
		throw IOException( string("stomp::io::SocketStream::write(char) - ") + buf );
	}
}

////////////////////////////////////////////////////////////////////////////////
void SocketStream::write( const char* buffer, const int len ) 
	throw (ActiveMQException)
{
	/*for( int ix=0; ix<len; ++ix ){
		char c = buffer[ix];
		if( c > 20 ){
			printf("%c", c );
		}
		else printf("[%d]", c );
	}*/
	
	int remaining = len;
	while( remaining > 0 ) {
      	
      	int length = send( socket->getHandle(), buffer, remaining, MSG_NOSIGNAL );      	
      	if( length < 0 ){
            socket->close();
      		char buf[500];
			strerror_r( errno, buf, 500 );
			throw IOException( string("stomp::io::SocketStream::write(char*,int) - ") + buf );
      	}
      	
      	buffer+=length;
      	remaining -= length;
	}
}

