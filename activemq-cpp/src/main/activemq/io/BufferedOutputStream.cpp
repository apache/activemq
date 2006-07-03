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
 
#include "BufferedOutputStream.h"
#include <algorithm>

using namespace activemq::io;
using namespace std;

////////////////////////////////////////////////////////////////////////////////
BufferedOutputStream::BufferedOutputStream( OutputStream* stream )
{
	// Default to 1k buffer.
	init( stream, 1024 );
}

////////////////////////////////////////////////////////////////////////////////
BufferedOutputStream::BufferedOutputStream( OutputStream* stream, 
	const int bufSize )
{
	init( stream, bufSize );
}

////////////////////////////////////////////////////////////////////////////////
BufferedOutputStream::~BufferedOutputStream()
{
    // Destroy the buffer.
    if( buffer != NULL ){
        delete [] buffer;
        buffer = NULL;
    }
}

////////////////////////////////////////////////////////////////////////////////
void BufferedOutputStream::init( OutputStream* stream, const int bufSize ){
	
	this->stream = stream;
	this->bufferSize = bufSize;
	
	buffer = new unsigned char[bufSize];
	head = tail = 0;
}

////////////////////////////////////////////////////////////////////////////////
void BufferedOutputStream::close() throw(cms::CMSException){
	
	// Flush this stream.
	flush();	
	
	// Close the delegate stream.
	stream->close();
}

////////////////////////////////////////////////////////////////////////////////
void BufferedOutputStream::emptyBuffer() throw (IOException){
	
	if( head != tail ){
		stream->write( buffer+head, tail-head );
	}
	head = tail = 0;
}
		
////////////////////////////////////////////////////////////////////////////////
void BufferedOutputStream::flush() throw (IOException){
	
	// Empty the contents of the buffer to the output stream.
	emptyBuffer();
	
	// Flush the output stream.
	stream->flush();
}

////////////////////////////////////////////////////////////////////////////////
void BufferedOutputStream::write( const unsigned char c ) throw (IOException){
	
	if( tail >= bufferSize ){
		emptyBuffer();
	}
	
	buffer[tail++] = c;	
}

////////////////////////////////////////////////////////////////////////////////		
void BufferedOutputStream::write( const unsigned char* buffer, const int len ) 
	throw (IOException)
{		
	// Iterate until all the data is written.
	for( int pos=0; pos < len; ){
		
		if( tail >= bufferSize ){
			emptyBuffer();
		}
	
		// Get the number of bytes left to write.
		int bytesToWrite = min( bufferSize-tail, len-pos );
		
		// Copy the data.
		memcpy( this->buffer+tail, buffer+pos, bytesToWrite );
		
		// Increase the tail position.
		tail += bytesToWrite;
		
		// Decrease the number of bytes to write.
		pos += bytesToWrite;	
	}	
}

