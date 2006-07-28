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
    #include <sys/poll.h>
    #include <sys/socket.h>
    #include <errno.h>
    extern int errno;
#else
    #include <Winsock2.h>
#endif

#include <activemq/network/SocketInputStream.h>
#include <activemq/io/IOException.h>
#include <stdlib.h>
#include <string>

using namespace activemq;
using namespace activemq::network;
using namespace activemq::io;
using namespace std;

////////////////////////////////////////////////////////////////////////////////
SocketInputStream::SocketInputStream( network::Socket::SocketHandle socket )
{
    this->socket = socket;
    debug = false;
}

////////////////////////////////////////////////////////////////////////////////
SocketInputStream::~SocketInputStream()
{
}

////////////////////////////////////////////////////////////////////////////////
int SocketInputStream::available() const{
   
   
#if defined(unix) && !defined(__CYGWIN__)
    
    // Poll the socket for input.
    pollfd fd;
    fd.fd = socket;
    fd.events = POLLIN;
    fd.revents = POLLIN;
    int status = poll( &fd, 1, 1 );
    if( status > 0 ){
        return 1;
    }
   
#else 

    // Poll instantaneously to see if there is data on the socket.
    timeval timeout;
    timeout.tv_sec = 0;
    timeout.tv_usec = 100;
   
    fd_set pollSet;
    FD_ZERO( &pollSet );
    FD_SET( 0, &pollSet );
    pollSet.fd_array[0] = socket;
    if( ::select( 1, &pollSet, NULL, NULL, &timeout) > 0 ){
        return 1;
    }
   
#endif

    return 0;
}

////////////////////////////////////////////////////////////////////////////////
unsigned char SocketInputStream::read() throw (IOException){
   
    unsigned char c;  
    int len = read( &c, 1 );
    if( len != sizeof(c) ){
        throw IOException( __FILE__, __LINE__, 
            "activemq::io::SocketInputStream::read - failed reading a byte");
    }
   
    return c;
}

////////////////////////////////////////////////////////////////////////////////
int SocketInputStream::read( unsigned char* buffer, const int bufferSize ) throw (IOException){
   
    int bytesAvailable = available();
   
    while( true )
    {
        int len = ::recv(socket, (char*)buffer, bufferSize, 0);
      
        // Check for typical error conditions.
        if( len < 0 )
        {
            #if defined(unix) && !defined(__CYGWIN__)
         
                // If the socket was temporarily unavailable - just try again.
                if( errno == EAGAIN ){
                    continue;
                }
             
                // Create the error string.
                char* errorString = ::strerror(errno);
             
            #else
            
                // If the socket was temporarily unavailable - just try again.
                int errorCode = ::WSAGetLastError();
                if( errorCode == WSAEWOULDBLOCK || errorCode == WSAETIMEDOUT ){
                    continue;
                }
          
                // Create the error string.
                static const int errorStringSize = 512;
                char errorString[errorStringSize];
                memset( errorString, 0, errorStringSize );
                FormatMessage(FORMAT_MESSAGE_FROM_SYSTEM,
                   0,
                   errorCode,
                   0,
                   errorString,
                   errorStringSize - 1,
                   NULL);
                  
            #endif
            
            // Otherwise, this was a bad error - throw an exception.
            throw IOException( __FILE__, __LINE__, 
                "activemq::io::SocketInputStream::read - %s", errorString );
        }
      
        // No error, but no data - check for a broken socket.
        if( len == 0 )
        {
            // If the poll showed data, but we failed to read any,
            // the socket is broken.
            if( bytesAvailable > 0 ){
                throw IOException( __FILE__, __LINE__, 
                    "activemq::io::SocketInputStream::read - The connection is broken" );
            }
         
            // Socket is not broken, just had no data.
            return 0;
        }
      
        if( debug ){
            printf("SocketInputStream:read(), numbytes:%d -", len);
            for( int ix=0; ix<len; ++ix ){
                if( buffer[ix] > 20 )
                    printf("%c", buffer[ix] );
                else
                    printf("[%d]", buffer[ix] );
            }
            printf("\n");
        }
        
        // Data was read successfully - return the bytes read.
        return len;
    }
}
