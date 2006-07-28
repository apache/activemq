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
#include "ppr/io/SocketOutputStream.hpp"

using namespace apache::ppr::io;

/*
 *
 */
SocketOutputStream::SocketOutputStream(p<ISocket> socket)
{
    this->socket = socket ;
}

/*
 *
 */
SocketOutputStream::~SocketOutputStream()
{
    // no-op
}

void SocketOutputStream::close() throw(IOException)
{
    // no-op
}

void SocketOutputStream::flush() throw(IOException)
{
    // no-op
}

/*
 *
 */
int SocketOutputStream::write(const char* buf, int index, int size) throw(IOException)
{
    const char*  buffer = buf + index ;
    int          length, remaining = size ;

    // Loop until requested number of bytes are read
    while( remaining > 0 )
    {
        // Try to write remaining bytes
        length = remaining ;

        try
        {
            // Write some bytes to socket
            length = socket->send(buffer, length) ;
        }
        catch( SocketException se )
        {
            // Exit on any error
            throw IOException("Failed to write to socket; error message = %s, at %s line %d", se.what(), __FILE__, __LINE__) ;
        }

        // Adjust buffer pointer and remaining number of bytes
        buffer    += length ;
        remaining -= length ;
	}
	return size ;
}
