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
 
#include "StompIO.h"

using namespace activemq::transport::stomp;
using namespace activemq::io;
using namespace std;

////////////////////////////////////////////////////////////////////////////////
StompIO::StompIO( InputStream* istream, OutputStream* ostream )
{
	this->istream = istream;
	this->ostream = ostream;
}

////////////////////////////////////////////////////////////////////////////////
StompIO::~StompIO()
{
}

////////////////////////////////////////////////////////////////////////////////
int StompIO::readStompHeaderLine( char* buf, const int bufLen ) throw (ActiveMQException){
	
	int pos = 0;
	
	while( pos < bufLen ){
		
		// Read the next char from the stream.
		buf[pos] = istream->read();
		
  		// Increment the position pointer.
  		pos++;
  		
  		// If we reached the line terminator, return the total number
  		// of characters read.
  		if( buf[pos-1] == '\n' ){
  			
  			// Overwrite the line feed with a null character. 
  			buf[pos-1] = '\0';	  			
  			return pos;
  		}
	}
	
	// Reading is not complete.
	return pos;
}

////////////////////////////////////////////////////////////////////////////////
int StompIO::readStompBodyLine( char* buf, const int bufLen ) throw (ActiveMQException){
	
    int content_length = 0;
   
   // Check for the content-length header.  This is optional - if not provided
   // we stop when we encounter a \0\n.
   const StompFrame::HeaderInfo* headerInfo = frame.getHeaderInfo(StompFrame::HEADER_CONTENTLENGTH);
   if( headerInfo != NULL )
   {
      const char* lengthProperty = headerInfo->value;
      char* stopped_string = NULL;

      content_length = strtoul(
         lengthProperty, 
         &stopped_string, 
         10);
   }
   
	int pos = 0;
	
	while( pos < bufLen ){
		
		// Read the next char from the stream.
		buf[pos] = istream->read();
		
  		// Increment the position pointer.
  		pos++;
        
        // Are we at the end of the frame?  The end frame pattern is \0\n
        bool foundFrameEndPattern = (pos >= 2 && buf[pos-2]=='\0' && buf[pos-1] == '\n'); 
        if( (pos > content_length) && foundFrameEndPattern ){                             
            return pos;
        }
	}
	
	// Reading is not complete.
	return pos;
}

////////////////////////////////////////////////////////////////////////////////
void StompIO::readStompCommand( StompFrame& frame ) throw (ActiveMQException){
	
	// The command is the first element in the message - initialize
	// the buffer position.
	readBufferPos = 0;
	
	// Read the command;
	int numChars = readStompHeaderLine( readBuffer, readBufferSize );
	if( readBufferPos + numChars >= readBufferSize ){
		throw ActiveMQException( "readStompCommand: exceeded buffer size" );
	}
	
	// Set the command in the frame - do not copy the memory.
	frame.setCommand( StompFrame::toCommand(readBuffer) );
	
	// Increment the position in the buffer.
	readBufferPos += numChars;		
}

////////////////////////////////////////////////////////////////////////////////
void StompIO::readStompHeaders( StompFrame& frame ) throw (ActiveMQException){
	
	// Read the command;
	bool endOfHeaders = false;
	while( !endOfHeaders ){
		
		// Read in the next header line.
		int numChars = readStompHeaderLine(
			readBuffer+readBufferPos, 
			readBufferSize-readBufferPos );		
		if( readBufferPos+numChars >= readBufferSize ){
			throw ActiveMQException( "readStompHeaders: exceeded buffer size" ); // should never get here
		}		
		if( numChars == 0 ){
			throw ActiveMQException( "readStompHeaders: no characters read" ); // should never get here
		}
		
		// Check for an empty line to demark the end of the header section.
		if( readBuffer[readBufferPos] == '\0' ){
			endOfHeaders = true;
		}	
		
		// Search through this line to separate the key/value pair.
		for( int ix=readBufferPos; ix<readBufferPos+numChars; ++ix ){
			
			// If found the key/value separator...
			if( readBuffer[ix] == ':' ){
				
				// Null-terminate the key.
				readBuffer[ix] = '\0'; 
				
				const char* key = readBuffer+readBufferPos;
				int keyLen = ix-readBufferPos;
				const char* value = readBuffer+ix+1;
				int valLen = numChars - keyLen - 2;
				
				// Assign the header key/value pair.
				frame.setHeader( key, 
					keyLen,
					value,
					valLen );
				
				// Break out of the for loop.
				break;
			}
		}
		
		// Point past this line in the buffer.
		readBufferPos+=numChars;	
	}	
}

////////////////////////////////////////////////////////////////////////////////
void StompIO::readStompBody( StompFrame& frame ) throw (ActiveMQException){
	
	// Read in the next header line.
	int numChars = readStompBodyLine( 
		readBuffer+readBufferPos, 
		readBufferSize-readBufferPos );	
	if( readBufferPos+numChars >= readBufferSize ){
		throw ActiveMQException( "readStompBody: exceeded buffer size" ); // should never get here
	}		
	if( numChars == 0 ){
		throw ActiveMQException( "readStompBody: no characters read" ); // should never get here
	}	
	
	// Set the body contents in the frame.
	frame.setBodyText( readBuffer+readBufferPos, numChars );
	
	// Point past this line in the buffer.
	readBufferPos+=numChars;	
}

////////////////////////////////////////////////////////////////////////////////
StompFrame* StompIO::readStompFrame() throw (ActiveMQException){
		
	// Read the command into the frame.
	readStompCommand( frame );
	
	// Read the headers.
	readStompHeaders( frame );
	
	// Read the body.
	readStompBody( frame );
	
	return &frame;
}

////////////////////////////////////////////////////////////////////////////////
void StompIO::writeStompFrame( StompFrame& frame ) throw( ActiveMQException ){
	
	// Write the command.
	StompFrame::Command cmd = frame.getCommand();
   	write( StompFrame::toString( cmd ), StompFrame::getCommandLength( cmd ) );
   	write( '\n' );
   
	// Write all the headers.
   	const StompFrame::HeaderInfo* headerInfo = frame.getFirstHeader();
   	for( ; headerInfo != NULL; headerInfo = frame.getNextHeader() ){
  		
   		write( headerInfo->key, headerInfo->keyLength );
   		write( ':' );
   		write( headerInfo->value, headerInfo->valueLength );
   		write( '\n' );   		
	}
   
   	// Finish the header section with a form feed.
   	write( '\n' );
   
   	// Write the body.
   	const char* body = frame.getBody();
   	if( body != NULL ) {
    	write( body, frame.getBodyLength() );
   	}
   	write( '\0' );
   	write( '\n' );
   	
   	// Flush the stream.
   	flush();
}

