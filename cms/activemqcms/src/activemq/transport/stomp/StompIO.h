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

#ifndef ACTIVEMQ_TRANSPORT_STOMP_STOMPIO_H_
#define ACTIVEMQ_TRANSPORT_STOMP_STOMPIO_H_
 
#include <activemq/transport/stomp/StompInputStream.h>
#include <activemq/transport/stomp/StompOutputStream.h>
#include <activemq/ActiveMQException.h>
#include <activemq/transport/stomp/StompFrame.h>

namespace activemq{
namespace transport{
namespace stomp{
	
    /**
     * Input/Output stream for stomp frames.
     * @author Nathan Mittler
     */
	class StompIO 
	: 
		public StompInputStream,
		public StompOutputStream
	{
	public:
	
		StompIO( io::InputStream* stream, io::OutputStream* ostream );
		virtual ~StompIO();
			
		virtual int available() const{ return istream->available(); }
		
		virtual char read() throw (ActiveMQException){ return istream->read(); }
		
		virtual int read( char* buffer, const int bufferSize ) throw (ActiveMQException){
			return istream->read( buffer, bufferSize );
		}
		
		virtual StompFrame* readStompFrame() throw (ActiveMQException);
		
		virtual void write( const char c ) throw (ActiveMQException){ 
			ostream->write( c ); 
		}
		
		virtual void write( const char* buffer, const int len ) throw (ActiveMQException){
			ostream->write( buffer, len );
		}
		
		virtual void flush() throw (ActiveMQException){ ostream->flush(); }
		
		virtual void writeStompFrame( StompFrame& frame ) throw( ActiveMQException );
		
		virtual void close() throw(cms::CMSException){ 
			istream->close(); 
			ostream->close();
		}
				
	private:
	
		int readStompHeaderLine( char* buf, const int bufLen ) throw (ActiveMQException);
		int readStompBodyLine( char* buf, const int bufLen ) throw (ActiveMQException);
		void readStompCommand( StompFrame& frame ) throw (ActiveMQException);
		void readStompHeaders( StompFrame& frame ) throw (ActiveMQException);
		void readStompBody( StompFrame& frame ) throw (ActiveMQException);
		
	private:
	
		// The streams.
		io::InputStream* istream;
		io::OutputStream* ostream;
		
		// The stomp frame for reads.
		StompFrame frame;
		
		// Make a 1-Meg buffer so we should never run out of space with a single frame.
		static const int readBufferSize = 1000000;
		char readBuffer[readBufferSize];
		int readBufferPos;
	};

}}}

#endif /*ACTIVEMQ_TRANSPORT_STOMP_STOMPIO_H_*/
