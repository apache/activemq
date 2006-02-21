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
#include "io/SocketBinaryReader.hpp"

using namespace apache::activemq::client::io;

/*
 *
 */
SocketBinaryReader::SocketBinaryReader(apr_socket_t* socket)
{
    this->socket = socket ;
}

/*
 *
 */
SocketBinaryReader::~SocketBinaryReader()
{
    // no-op
}

void SocketBinaryReader::close() throw(IOException)
{
    // no-op
}

/*
 *
 */
int SocketBinaryReader::read(char* buffer, int size) throw(IOException)
{
    apr_size_t   length, remaining = size ;
    apr_status_t rc ;

    // Loop until requested number of bytes are read
    while( remaining > 0 )
    {
        // Try to read remaining bytes
        length = remaining ;

        // Read some bytes from socket
        rc = apr_socket_recv(socket, buffer, &length) ;

        // Adjust buffer pointer and remaining number of bytes
        buffer    += length ;
        remaining -= length ;

        // Exit on any error
        if( rc != APR_SUCCESS )
        {
            string message ;
            char buf[10] ;

            // Construct error message
            message.assign("Failed to read from socket. Code = ") ;
            message.append( itoa(rc, buf, 10) ) ;

            throw IOException(__FILE__, __LINE__, message.c_str()) ;
        }
	}
	return (int)size ;
}
