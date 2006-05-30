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
 
#include "BufferedInputStream.h"
#include <algorithm>

using namespace activemq::io;
using namespace std;

////////////////////////////////////////////////////////////////////////////////
BufferedInputStream::BufferedInputStream( InputStream* stream )
{
	// Default to a 1k buffer.
	init( stream, 1024 );
}

////////////////////////////////////////////////////////////////////////////////
BufferedInputStream::BufferedInputStream( InputStream* stream, 
	const int bufferSize )
{
	init( stream, bufferSize );
}

////////////////////////////////////////////////////////////////////////////////
BufferedInputStream::~BufferedInputStream()
{
    // Destroy the buffer.
    if( buffer != NULL ){
        delete [] buffer;
        buffer = NULL;
    }
}

////////////////////////////////////////////////////////////////////////////////
void BufferedInputStream::init( InputStream* stream, const int bufferSize ){
	
	this->stream = stream;
	this->bufferSize = bufferSize;
	
	// Create the buffer and initialize the head and tail positions.
	buffer = new char[bufferSize];
	head = 0;
	tail = 0;
}

////////////////////////////////////////////////////////////////////////////////
void BufferedInputStream::close() throw(cms::CMSException){
	
	// Close the delegate stream.
	stream->close();
}

////////////////////////////////////////////////////////////////////////////////
char BufferedInputStream::read() throw (ActiveMQException){
	
	// If we don't have any data buffered yet - read as much as we can.	
	if( tail == head ){
		bufferData();
	}
	
	// Get the next character.
	char returnValue = buffer[head++];
	
	// If the buffer is now empty - reset it to the beginning of the buffer.
	if( tail == head ){
		tail = head = 0;
	}
	
	return returnValue;
}

////////////////////////////////////////////////////////////////////////////////
int BufferedInputStream::read( char* buffer, 
	const int bufferSize ) throw (ActiveMQException){
	
	// If we still haven't filled the output buffer AND there is data
	// on the input stream to be read, read a buffer's
	// worth from the stream.
	int totalRead = 0;
	while( totalRead < bufferSize ){		
		
		// Get the remaining bytes to copy.
		int bytesToCopy = min( tail-head, (bufferSize-totalRead) );
		
		// Copy the data to the output buffer.	
		memcpy( buffer+totalRead, this->buffer+head, bytesToCopy );
		
		// Increment the total bytes read.
		totalRead += bytesToCopy;
		
		// Increment the head position.  If the buffer is now empty,
		// reset the positions and buffer more data.
		head += bytesToCopy;
		if( head == tail ){
			
			// Reset the buffer indicies.
			head = tail = 0;
			
			// If there is no more data currently available on the 
			// input stream, stop the loop.
			if( stream->available() == 0 ){
				break;
			}
			
			// Buffer as much data as we can.
			bufferData();
		}				
	}
	
	// Return the total number of bytes read.
	return totalRead;
}

////////////////////////////////////////////////////////////////////////////////
void BufferedInputStream::bufferData() throw (ActiveMQException){
	
	if( tail == bufferSize ){
		throw ActiveMQException( "BufferedInputStream::bufferData - buffer full" );
	}
	
	// Read in as many bytes as we can.
	int bytesRead = stream->read( buffer+tail, bufferSize-tail );
	if( bytesRead == 0 ){
		throw ActiveMQException("BufferedInputStream::read() - failed reading bytes from stream");
	}
	
	// Increment the tail to the new end position.
	tail += bytesRead;
}
