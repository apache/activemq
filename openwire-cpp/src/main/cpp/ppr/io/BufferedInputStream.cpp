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
#include "ppr/io/BufferedInputStream.hpp"

using namespace apache::ppr::io;

/*
 * 
 */
BufferedInputStream::BufferedInputStream(p<IInputStream> istream)
{
    this->istream = istream ;
    this->size    = DEFAULT_SIZE ;
	buffer        = new char[size] ;
	position      = 0 ;
    treshold      = 0 ;
}

/*
 * 
 */
BufferedInputStream::BufferedInputStream(p<IInputStream> istream, int size)
{
    this->istream = istream ;
    this->size    = size ;
	buffer        = new char[size] ;
	position      = 0 ;
    treshold      = 0 ;
}

/*
 * Close stream.
 */
void BufferedInputStream::close() throw(IOException)
{
    // Cascade close request to underlying stream
    if( istream != NULL )
    {
        istream->close() ;
        istream = NULL ;
        buffer  = NULL ;
    }
}

/*
 *
 */
int BufferedInputStream::read(char* buf, int offset, int length) throw(IOException)
{
    // Check if underlying stream has been closed
    checkClosed() ;

    // Assert parameters
    if( buf == NULL || offset < 0 || offset > length || length < 0 )
        throw IllegalArgumentException() ;
    
    // Skip read if length is invalid
    if( length == 0 )
        return 0 ;

    // Have we reached end-of-buffer?
	if( isEOB() )
    {
        // Skip buffering should request be larger than internal buffer
	    if( length >= size )
		    return istream->read(buf, offset, length) ;

        // Load internal buffer with new data
	    loadBuffer() ;
	}
    // Any data available?
	if( isEOB() )
        return -1 ;

    // Copy requested bytes up to max buffer size
	int bytesRead = min(length, treshold - position) ;

    // Copy read bytes into supplied buffer
	memcpy(&buf[offset], &buffer[position], bytesRead) ;

    // Adjust array position
	position += bytesRead ;

	return bytesRead ;
}

/*
 * Load the input buffer with new data.
 */
void BufferedInputStream::loadBuffer() throw(IOException)
{
    // Try to load the whole buffer
    int bytesRead = istream->read(buffer, 0, size) ;

    // Reset counters if any data was loaded
    if( bytesRead > 0 )
    {
        treshold = bytesRead ;
        position = 0 ;
    }
}

/*
 * Check if stream has been closed.
 */
void BufferedInputStream::checkClosed() throw(IOException)
{
    if( istream == NULL )
        throw IOException("Input stream closed") ;
}

/*
 * Check is end-of-buffer has been reached.
 */
bool BufferedInputStream::isEOB()
{
    return ( position >= treshold ) ? true : false ;
}
