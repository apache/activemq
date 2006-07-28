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
#include "ppr/io/SocketInputStream.hpp"

using namespace apache::ppr::io;

/*
 *
 */
SocketInputStream::SocketInputStream(p<ISocket> socket)
{
    this->socket = socket ;
}

/*
 *
 */
SocketInputStream::~SocketInputStream()
{
    // no-op
}

/*
 *
 */
void SocketInputStream::close() throw(IOException)
{
    // Cascade close request to underlying socket
    if( socket != NULL )
    {
        socket->close() ;
        socket = NULL ;
    }
}

/*
 *
 */
int SocketInputStream::read(char* buf, int offset, int length) throw(IOException)
{
    char* buffer = buf + offset ;
    //char* buffer = buf + index ;
    //int   length, remaining = size ;
    int bytesRead ;

    // Loop until requested number of bytes are read
    //while( remaining > 0 )
    //{
        // Try to read remaining bytes
      //  length = remaining ;

        try
        {
            // Read some bytes from socket
            //length = socket->receive(buffer, length) ;
            bytesRead = socket->receive(buffer, length) ;
        }
        catch( SocketException se )
        {
            // Exit on any error
            throw IOException("Failed to read from socket; error message = %s, at %s line %d", se.what(), __FILE__, __LINE__) ;
        }

        // Adjust buffer pointer and remaining number of bytes
        //buffer    += length ;
        //remaining -= length ;
	//}
	//return size ;
    return bytesRead ;
}
