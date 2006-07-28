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
#include "ppr/io/BufferedOutputStream.hpp"

using namespace apache::ppr::io;

/*
 * 
 */
BufferedOutputStream::BufferedOutputStream(p<IOutputStream> ostream)
{
    this->ostream = ostream ;
    this->size    = DEFAULT_SIZE ;
	buffer        = new char[size] ;
	position      = 0 ;
    treshold      = size ;
}

/*
 * 
 */
BufferedOutputStream::BufferedOutputStream(p<IOutputStream> ostream, int size)
{
    this->ostream = ostream ;
    this->size    = size ;
	buffer        = new char[size] ;
	position      = 0 ;
    treshold      = size ;
}

/*
 * Close stream.
 */
void BufferedOutputStream::close() throw(IOException)
{
    // Cascade close request to internal stream
    if( ostream != NULL )
    {
        // Flush any remaining bytes
        flush0() ;

        // Shut down
        ostream->close() ;
        ostream = NULL ;
        buffer  = NULL ;
    }
}

/*
 *
 */
int BufferedOutputStream::write(const char* buf, int offset, int length) throw(IOException)
{
    // Check if underlying stream has been closed
    checkClosed() ;

    // Assert parameters
    if( buf == NULL || offset < 0 || offset > length || length < 0 )
        throw IllegalArgumentException() ;

    // Skip write if length is invalid
    if( length == 0 )
        return 0 ;

    // Skip buffering should request be larger than internal buffer
    if( length >= size )
    {
        flush0() ;
	    return ostream->write(buf, offset, length) ;
    }
    int start = offset, end = offset + length ;

    while( start < end )
    {
        int delta = min(treshold - position, end - start) ;
        memcpy(&buffer[position], &buf[start], delta) ;
        start    += delta ;
        position += delta ;

        // Have we reached end-of-buffer?
	    if( isEOB() )
            flush0() ;
    }
    return length ;
}

/*
 * Flush stream, i.e. buffer.
 */
void BufferedOutputStream::flush() throw(IOException)
{
    // Check if underlying stream has been closed
    checkClosed() ;

    flush0() ;
}

/*
 * Flush buffer.
 */
void BufferedOutputStream::flush0() throw(IOException)
{
    // Check if there is anything to flush
    if( position > 0 )
    {
        ostream->write(buffer, 0, position) ;
	    position = 0 ;
	}
    // Flush underlying stream
    ostream->flush() ;
}

/*
 * Check if stream has been closed.
 */
void BufferedOutputStream::checkClosed() throw(IOException)
{
    if( ostream == NULL )
        throw IOException("Output stream closed") ;
}

/*
 * Check is end-of-buffer has been reached.
 */
bool BufferedOutputStream::isEOB()
{
    return ( position >= treshold ) ? true : false ;
}
