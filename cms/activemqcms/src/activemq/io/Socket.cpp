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
 
#include "Socket.h"
#include "SocketStream.h"
#include <string>
#include <sys/types.h>
#include <netdb.h>
#include <errno.h>

using namespace activemq::io;
using namespace std;

////////////////////////////////////////////////////////////////////////////////
Socket::Socket()
{
	init();
}

////////////////////////////////////////////////////////////////////////////////
Socket::Socket( const char* host, const int port ) throw(IOException)
{
	init();
	
	// Now try connecting.
	connect( host, port );
}

////////////////////////////////////////////////////////////////////////////////
Socket::~Socket()
{
	// Close the socket if not already closed.
	close();	
}

////////////////////////////////////////////////////////////////////////////////
void Socket::init(){
	
	m_socket = -1;
	inputStream = NULL;
	outputStream = NULL;
	memset ( &addressIn, 0, sizeof ( addressIn ) );
}

////////////////////////////////////////////////////////////////////////////////
void Socket::connect( const char* host, const int port ) throw(IOException){
	
	try{
		// Close if not closed already.
		close();
		
		// Create the socket.
		m_socket = socket( AF_INET, SOCK_STREAM, 0 );
		if( m_socket == -1 ){
			throw IOException( string("stomp::io::Socket::connect - ") + strerror(errno) );
		}
		
		addressIn.sin_family = AF_INET;
  		addressIn.sin_port = htons ( port );

		// Create a network address structure.
  		if( inet_pton ( AF_INET, host, &addressIn.sin_addr ) < 0 ){
			throw IOException( string("stomp::io::Socket::connect - ") + strerror(errno) );
  		}

		// Connect the socket.
  		int status = ::connect ( m_socket, (sockaddr*)&addressIn, sizeof ( addressIn ) );
  		if( status == -1 ){            
			throw IOException( string("stomp::io::Socket::connect - ") + strerror(errno) );
  		}
		
		// Create an input/output stream for this socket.
		SocketStream* stream = new SocketStream( this );
		inputStream = stream;
		outputStream = stream;
		
	}catch( IOException& ex ){
		
		// Close the socket.
		throw ex;
	}
	catch( ... ){		
		
		throw IOException( "stomp::io::Socket::connect - caught unknown exception" );
	}
}

////////////////////////////////////////////////////////////////////////////////
void Socket::close() throw(cms::CMSException){
	
	// Destroy the input/output stream.
	try{
		
		if( inputStream != NULL ){
			delete inputStream;
		}
		
	}catch( ... ){};
	
	if( m_socket >= 0 ){
		
		// Shutdown the socket.
		::shutdown( m_socket, SHUT_RDWR );
		
		// Close the socket.
		::close( m_socket );
	}
	
	// Reinitialize all pointers.
	init();
}

////////////////////////////////////////////////////////////////////////////////
/*const char* Socket::getLocalHost() const{
	
	if( localAddress == NULL ){
		return NULL;
	}
	
	// Get the local ip.
	char* localIp;
   	apr_status_t rc = apr_sockaddr_ip_get(&localIp, localAddress);
	if( rc != APR_SUCCESS ){
		return NULL;
	}
	
	return localIp;
}

////////////////////////////////////////////////////////////////////////////////
int Socket::getLocalPort() const{
	
	if( localAddress == NULL ){
		return -1;
	}
	
	return localAddress->port;
	
}

////////////////////////////////////////////////////////////////////////////////
const char* Socket::getRemoteHost() const{
	
	if( remoteAddress == NULL ){
		return NULL;
	}
	
	// Get the remote ip.
	char* remoteIp;
   	apr_status_t rc = apr_sockaddr_ip_get(&remoteIp, remoteAddress);
	if( rc != APR_SUCCESS ){
		return NULL;
	}
	
	return remoteIp;
}

////////////////////////////////////////////////////////////////////////////////
int Socket::getRemotePort() const{
	
	if( remoteAddress == NULL ){
		return -1;
	}
	
	return remoteAddress->port;
}
*/

////////////////////////////////////////////////////////////////////////////////
int Socket::getSoLinger() const throw(SocketException){
	
	linger value;
	socklen_t length = sizeof(value);
	getsockopt(m_socket, SOL_SOCKET, SO_LINGER, &value, &length );
	
	return value.l_onoff? value.l_linger : 0;
}

////////////////////////////////////////////////////////////////////////////////
void Socket::setSoLinger( const int dolinger ) throw(SocketException){
	
	linger value;
	value.l_onoff = dolinger != 0;
	value.l_linger = dolinger;
	setsockopt(m_socket, SOL_SOCKET, SO_LINGER, &value, sizeof(value) );
}

////////////////////////////////////////////////////////////////////////////////
bool Socket::getKeepAlive() const throw(SocketException){
	
	int value;
	socklen_t length = sizeof(int);
	getsockopt(m_socket, SOL_SOCKET, SO_KEEPALIVE, &value, &length );
	return value != 0;
}

////////////////////////////////////////////////////////////////////////////////
void Socket::setKeepAlive( const bool keepAlive ) throw(SocketException){
	
	int value = keepAlive? 1 : 0;
	setsockopt(m_socket, SOL_SOCKET, SO_KEEPALIVE, &value, sizeof(int) );
}

////////////////////////////////////////////////////////////////////////////////
/*bool Socket::getTcpNoDelay() const throw(SocketException){
	
	apr_int32_t on;
	apr_status_t rc = apr_socket_opt_get( socket, APR_TCP_NODELAY, &on );
	if( rc != APR_SUCCESS ){
		throw SocketException( ErrorFactory::createErrorStr( "stomp::io::Socket::getTcpNoDelay()", rc ) );
	}
	
	return on != 0;
}

////////////////////////////////////////////////////////////////////////////////
void Socket::setTcpNoDelay( const bool noDelay ) throw(SocketException){
	
	apr_status_t rc = apr_socket_opt_set( socket, APR_TCP_NODELAY, noDelay? 1 : 0 );
	if( rc != APR_SUCCESS ){
		throw SocketException( ErrorFactory::createErrorStr( "stomp::io::Socket::setTcpNoDelay()", rc ) );
	}
}*/

////////////////////////////////////////////////////////////////////////////////
int Socket::getReceiveBufferSize() const throw(SocketException){
	
	int value;
	socklen_t length = sizeof(int);
	getsockopt(m_socket, SOL_SOCKET, SO_RCVBUF, &value, &length );
	return value;
}

////////////////////////////////////////////////////////////////////////////////
void Socket::setReceiveBufferSize( const int size ) throw(SocketException){
	
	setsockopt(m_socket, SOL_SOCKET, SO_RCVBUF, &size, sizeof(int) );
}

////////////////////////////////////////////////////////////////////////////////
bool Socket::getReuseAddress() const throw(SocketException){
	
	int value;
	socklen_t length = sizeof(int);
	getsockopt(m_socket, SOL_SOCKET, SO_REUSEADDR, &value, &length );
	return value != 0;
}

////////////////////////////////////////////////////////////////////////////////
void Socket::setReuseAddress( const bool reuse ) throw(SocketException){
	
	int value = reuse? 1 : 0;
	setsockopt(m_socket, SOL_SOCKET, SO_REUSEADDR, &value, sizeof(int) );
}

////////////////////////////////////////////////////////////////////////////////
int Socket::getSendBufferSize() const throw(SocketException){
	
	int value;
	socklen_t length = sizeof(int);
	getsockopt(m_socket, SOL_SOCKET, SO_SNDBUF, &value, &length );
	return value;
}

////////////////////////////////////////////////////////////////////////////////
void Socket::setSendBufferSize( const int size ) throw(SocketException){
	
	setsockopt(m_socket, SOL_SOCKET, SO_SNDBUF, &size, sizeof(int) );
}

////////////////////////////////////////////////////////////////////////////////
int Socket::getSoReceiveTimeout() const throw(SocketException){
	
	timeval value;
	socklen_t length = sizeof(value);
	getsockopt(m_socket, SOL_SOCKET, SO_RCVTIMEO, &value, &length );
	
	int microseconds = (value.tv_sec * 1000000) + value.tv_usec;
	return microseconds;
}

////////////////////////////////////////////////////////////////////////////////
void Socket::setSoReceiveTimeout( const int timeout ) throw(SocketException){
	
	timeval value;
	value.tv_sec = timeout / 1000000;
	value.tv_usec = timeout - (value.tv_sec * 1000000);
	setsockopt(m_socket, SOL_SOCKET, SO_RCVTIMEO, &value, sizeof(value) );
}

////////////////////////////////////////////////////////////////////////////////
int Socket::getSoSendTimeout() const throw(SocketException){
	
	timeval value;
	socklen_t length = sizeof(value);
	getsockopt(m_socket, SOL_SOCKET, SO_SNDTIMEO, &value, &length );
	
	int microseconds = (value.tv_sec * 1000000) + value.tv_usec;
	return microseconds;
}

////////////////////////////////////////////////////////////////////////////////
void Socket::setSoSendTimeout( const int timeout ) throw(SocketException){
		
	timeval value;
	value.tv_sec = timeout / 1000000;
	value.tv_usec = timeout - (value.tv_sec * 1000000);
	setsockopt(m_socket, SOL_SOCKET, SO_SNDTIMEO, &value, sizeof(value) );
}

////////////////////////////////////////////////////////////////////////////////
/*void Socket::setNonBlocking ( const bool nonBlocking )
{
  	int opts = fcntl ( m_sock, F_GETFL );
  	if ( opts < 0 )
    {
      return;
    }

  	if ( nonBlocking )
    	opts = ( opts | O_NONBLOCK );
  	else
    	opts = ( opts & ~O_NONBLOCK );

  	fcntl ( m_sock, F_SETFL, opts );
}*/
