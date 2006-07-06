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
#include "ppr/io/ByteArrayInputStream.hpp"

using namespace apache::ppr::io;

/*
 *
 */
ByteArrayInputStream::ByteArrayInputStream(array<char> buffer)
{
    this->body     = (buffer != NULL) ? buffer : NULL ;
    this->bodySize = (buffer != NULL) ? (int)body.size() : 0 ;
    this->offset   = 0 ;
}

/*
 *
 */
ByteArrayInputStream::~ByteArrayInputStream()
{
    // no-op
}

void ByteArrayInputStream::close() throw(IOException)
{
    // no-op
}

/*
 *
 */
array<char> ByteArrayInputStream::toArray()
{
    array<char> retBody ;

    // Create return body and copy current contents
    retBody = array<char> (bodySize) ;
    memcpy(retBody.c_array(), body.c_array(), bodySize) ;

    return retBody ;
}

/*
 *
 */
int ByteArrayInputStream::read(char* buf, int index, int length) throw(IOException)
{
    // Check for EOF offset
    if( offset > bodySize )
        throw EOFException() ;

    // Copy bytes from supplied buffer
    for( int i = index ; i < length ; i++, offset++ )
    {
        // Check for body EOF
        if( offset > bodySize )
            return length - i ;

        buf[i] = body[offset] ;
    }
    return length ;
}
