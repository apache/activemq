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

#if (defined(unix) || defined(__APPLE__)) && !defined(__CYGWIN__)
    #include <unistd.h>
    #include <netdb.h>
    #include <fcntl.h>
    #include <sys/file.h>
    #include <sys/socket.h>
    #include <netinet/in.h>
    #include <arpa/inet.h>
    extern int errno;
#else
    #include <Winsock2.h>
    #include <Ws2tcpip.h> 
    #include <sys/stat.h>
    #define stat _stat
#endif

#include <stdio.h>
#include <stdlib.h>
#include <stdarg.h>
#include <ctype.h>
#include <errno.h>
#include <sys/types.h>
#include "TcpSocket.h"
#include "SocketInputStream.h"
#include "SocketOutputStream.h"
#include <errno.h>

using namespace activemq::network;
using namespace activemq::io;

#if !defined( unix ) || defined( __CYGWIN__ )

    // Static socket initializer needed for winsock

    TcpSocket::StaticSocketInitializer::StaticSocketInitializer() {
        socketInitError = NULL;
        const WORD version_needed = MAKEWORD(2,2); // lo-order byte: major version
        WSAData temp;       
        if( WSAStartup( version_needed, &temp ) ){
           clear();
           socketInitError = new SocketException ( __FILE__, __LINE__,
               "winsock.dll was not found");
        }
    }
    TcpSocket::StaticSocketInitializer::~StaticSocketInitializer() {
        clear();
        WSACleanup();
    }
   
    // Create static instance of the socket initializer.
    TcpSocket::StaticSocketInitializer TcpSocket::staticSocketInitializer;
   
#endif

////////////////////////////////////////////////////////////////////////////////
TcpSocket::TcpSocket() {
   
    socketHandle = INVALID_SOCKET_HANDLE;
    inputStream = NULL;
    outputStream = NULL;
   
    #if !defined( unix ) || defined( __CYGWIN__ )
        if( staticSocketInitializer.getSocketInitError() != NULL ) {
            throw *staticSocketInitializer.getSocketInitError();
        }
    #endif
}

////////////////////////////////////////////////////////////////////////////////
TcpSocket::TcpSocket( Socket::SocketHandle socketHandle ){
    this->socketHandle = socketHandle;
   
    inputStream = new SocketInputStream( socketHandle );
    outputStream = new SocketOutputStream( socketHandle );
}

////////////////////////////////////////////////////////////////////////////////
TcpSocket::~TcpSocket()
{
    // No shutdown, just close - dont want blocking destructor.
    close();
}

////////////////////////////////////////////////////////////////////////////////
InputStream* TcpSocket::getInputStream(){
    return inputStream;
}

////////////////////////////////////////////////////////////////////////////////
OutputStream* TcpSocket::getOutputStream(){
    return outputStream;
}

////////////////////////////////////////////////////////////////////////////////
void TcpSocket::connect(const char* host, int port) throw ( SocketException )
{
    if( isConnected() ) {
        throw SocketException( __FILE__, __LINE__, 
            "Socket::connect - Socket already connected.  host: %s, port: %d", host, port );
    }
    
    // Create the socket.
    socketHandle = ::socket(AF_INET, SOCK_STREAM, 0);
    if( socketHandle < 0 ) {
        socketHandle = INVALID_SOCKET_HANDLE;
            throw SocketException( __FILE__, __LINE__, ::strerror( errno ) );
    }
   
    // Check port value.
    if (port <= 0 || port > 65535) {
        close();
        throw SocketException ( __FILE__, __LINE__, 
            "Socket::connect- Port out of range: %d", port );
    }
    
    sockaddr_in target_addr;
    target_addr.sin_family = AF_INET;
    target_addr.sin_port = htons( ( short ) port );
    target_addr.sin_addr.s_addr = 0; // To be set later down...
    memset( &target_addr.sin_zero, 0, sizeof( target_addr.sin_zero ) );

    // Resolve name
    addrinfo hints;
    memset(&hints, 0, sizeof(addrinfo));
    hints.ai_family = PF_INET;
    struct addrinfo *res_ptr = NULL;
    
    int status = ::getaddrinfo( host, NULL, &hints, &res_ptr );
    if( status != 0 || res_ptr == NULL){      
        throw SocketException( __FILE__, __LINE__, 
            "Socket::connect - %s", ::strerror( errno ) );        
    }
    
    assert(res_ptr->ai_addr->sa_family == AF_INET);
    // Porting: On both 32bit and 64 bit systems that we compile to soo far, sin_addr 
    // is a 32 bit value, not an unsigned long.
    assert( sizeof( ( ( sockaddr_in* )res_ptr->ai_addr )->sin_addr.s_addr ) == 4 );
    target_addr.sin_addr.s_addr = ( ( sockaddr_in* )res_ptr->ai_addr )->sin_addr.s_addr;
    freeaddrinfo( res_ptr );
   
    // Attempt the connection to the server.
    status = ::connect( socketHandle, 
                        ( const sockaddr * )&target_addr, 
                        sizeof( target_addr ) );
                      
    if( status < 0 ){
        close();
        throw SocketException( __FILE__, __LINE__, 
            "Socket::connect - %s", ::strerror( errno ) );
    }
   
    // Create an input/output stream for this socket.
    inputStream = new SocketInputStream( socketHandle );
    outputStream = new SocketOutputStream( socketHandle );
}

////////////////////////////////////////////////////////////////////////////////
void TcpSocket::close() throw( cms::CMSException )
{
    // Destroy the input stream.
    if( inputStream != NULL ){
        delete inputStream;
        inputStream = NULL;
    }
   
    // Destroy the output stream.
    if( outputStream != NULL ){
        delete outputStream;
        outputStream = NULL;
    }
   
    if( isConnected() )
    {
        ::shutdown( socketHandle, 2 );
        
        #if defined(unix) && !defined(__CYGWIN__)
            ::close( socketHandle );
        #else
           ::closesocket( socketHandle );
        #endif
      
       socketHandle = INVALID_SOCKET_HANDLE;
    }
}

////////////////////////////////////////////////////////////////////////////////
int TcpSocket::getSoLinger() const throw( SocketException ){
   
    linger value;
    socklen_t length = sizeof( value );
    ::getsockopt( socketHandle, SOL_SOCKET, SO_LINGER, (char*)&value, &length );
   
    return value.l_onoff? value.l_linger : 0;
}

////////////////////////////////////////////////////////////////////////////////    
void TcpSocket::setSoLinger( const int dolinger ) throw( SocketException ){
   
    linger value;
    value.l_onoff = dolinger != 0;
    value.l_linger = dolinger;
    ::setsockopt( socketHandle, SOL_SOCKET, SO_LINGER, (char*)&value, sizeof(value) );
}

////////////////////////////////////////////////////////////////////////////////
bool TcpSocket::getKeepAlive() const throw( SocketException ){
   
    int value;
    socklen_t length = sizeof( int );
    ::getsockopt( socketHandle, SOL_SOCKET, SO_KEEPALIVE, (char*)&value, &length );
    return value != 0;
}

////////////////////////////////////////////////////////////////////////////////
void TcpSocket::setKeepAlive( const bool keepAlive ) throw( SocketException ){
   
    int value = keepAlive? 1 : 0;
    ::setsockopt(socketHandle, SOL_SOCKET, SO_KEEPALIVE, (char*)&value, sizeof(int) );
}

////////////////////////////////////////////////////////////////////////////////
int TcpSocket::getReceiveBufferSize() const throw( SocketException ){
   
    int value;
    socklen_t length = sizeof( int );
    ::getsockopt( socketHandle, SOL_SOCKET, SO_RCVBUF, (char*)&value, &length );
    return value;
}

////////////////////////////////////////////////////////////////////////////////
void TcpSocket::setReceiveBufferSize( const int size ) throw( SocketException ){
   
    ::setsockopt( socketHandle, SOL_SOCKET, SO_RCVBUF, (char*)&size, sizeof(int) );
}

////////////////////////////////////////////////////////////////////////////////
bool TcpSocket::getReuseAddress() const throw( SocketException ){
   
    int value;
    socklen_t length = sizeof( int );
    ::getsockopt( socketHandle, SOL_SOCKET, SO_REUSEADDR, (char*)&value, &length );
    return value != 0;
}

////////////////////////////////////////////////////////////////////////////////
void TcpSocket::setReuseAddress( const bool reuse ) throw( SocketException ){
   
    int value = reuse? 1 : 0;
    ::setsockopt( socketHandle, SOL_SOCKET, SO_REUSEADDR, (char*)&value, sizeof(int) );
}

////////////////////////////////////////////////////////////////////////////////
int TcpSocket::getSendBufferSize() const throw( SocketException ){
   
    int value;
    socklen_t length = sizeof( int );
    ::getsockopt( socketHandle, SOL_SOCKET, SO_SNDBUF, (char*)&value, &length );
    return value;
}

////////////////////////////////////////////////////////////////////////////////
void TcpSocket::setSendBufferSize( const int size ) throw( SocketException ){
   
    ::setsockopt( socketHandle, SOL_SOCKET, SO_SNDBUF, ( char* )&size, sizeof( int ) );
}

////////////////////////////////////////////////////////////////////////////////
void TcpSocket::setSoTimeout ( const int millisecs ) throw ( SocketException )
{
    #if defined( unix ) && !defined( __CYGWIN__ )
        timeval timot;
        timot.tv_sec = millisecs / 1000;
        timot.tv_usec = (millisecs % 1000) * 1000;
    #else
        int timot = millisecs;
    #endif

    ::setsockopt( socketHandle, SOL_SOCKET, SO_RCVTIMEO, (const char*) &timot, sizeof (timot) );
    ::setsockopt( socketHandle, SOL_SOCKET, SO_SNDTIMEO, (const char*) &timot, sizeof (timot) );
}

////////////////////////////////////////////////////////////////////////////////
int TcpSocket::getSoTimeout() const throw( SocketException )
{
    #if defined( unix ) && !defined( __CYGWIN__ )
        timeval timot;
        timot.tv_sec = 0;
        timot.tv_usec = 0;
        socklen_t size = sizeof(timot);
    #else
        int timot = 0;
        int size = sizeof(timot);
    #endif
  
    ::getsockopt(socketHandle, SOL_SOCKET, SO_RCVTIMEO, (char*) &timot, &size);
  
    #if defined( unix ) && !defined( __CYGWIN__ )
        return (timot.tv_sec * 1000) + (timot.tv_usec / 1000);
    #else
        return timot;
    #endif

}
