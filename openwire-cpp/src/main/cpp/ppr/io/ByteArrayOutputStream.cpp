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
#include "ppr/io/ByteArrayOutputStream.hpp"

using namespace apache::ppr::io;

/*
 *
 */
ByteArrayOutputStream::ByteArrayOutputStream()
{
    this->body       = array<char> (INITIAL_SIZE) ;
    this->bodySize   = INITIAL_SIZE ;
    this->bodyLength = 0 ;
    this->offset     = 0 ;
}

/*
 *
 */
ByteArrayOutputStream::ByteArrayOutputStream(array<char> buffer)
{
    this->body       = (buffer != NULL ) ? buffer : array<char> (INITIAL_SIZE) ;
    this->bodySize   = (int)body.size() ;
    this->bodyLength = 0 ;
    this->offset     = 0 ;
}

/*
 *
 */
ByteArrayOutputStream::~ByteArrayOutputStream()
{
    // no-op
}

/*
 *
 */
array<char> ByteArrayOutputStream::toArray()
{
    // Return NULL if no array content
    if( bodyLength == 0 ) 
        return NULL ;

    // Create return body and copy current contents
    array<char> retBody = array<char> (bodyLength) ;
    memcpy(retBody.c_array(), body.c_array(), bodyLength) ;

    return retBody ;
}

/*
 *
 */
void ByteArrayOutputStream::close() throw(IOException)
{
    // no-op
}

/*
 *
 */
void ByteArrayOutputStream::flush() throw(IOException)
{
    // no-op
}

/*
 *
 */
int ByteArrayOutputStream::write(const char* buf, int index, int length) throw(IOException)
{
    // Assert buffer parameter
    if( buf == NULL )
        return 0 ;

    // Copy bytes from supplied buffer
    for( int i = index ; i < length ; i++, offset++ )
    {
        // Check for EOF offset
        if( offset > bodySize )
            expandBody() ;

        body[offset] = buf[i] ;
    }
    // Update content length
    bodyLength = offset ;

    return length ;
}

/*
 *
 */
void ByteArrayOutputStream::expandBody()
{
    array<char> newBody ;
    int         newSize = bodySize + ByteArrayOutputStream::EXPAND_SIZE ;

    // Create new body and copy current contents
    newBody = array<char> (newSize) ;
    memcpy(newBody.c_array(), body.c_array(), bodySize) ;

    // Clean up and use new content body
    body     = newBody ;
    bodySize = newSize ;
}
