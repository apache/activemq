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

#ifndef ACTIVEMQ_IO_BUFFEREDINPUTSTREAM_H_
#define ACTIVEMQ_IO_BUFFEREDINPUTSTREAM_H_
 
#include <activemq/io/InputStream.h>

namespace activemq{
namespace io{
	
	class BufferedInputStream : public InputStream
	{
	public:
	
		BufferedInputStream( InputStream* stream );
		BufferedInputStream( InputStream* stream, const int bufferSize );
		virtual ~BufferedInputStream();
		
		virtual int available() const{	
			return (tail-head)+stream->available();
		}
		
		virtual char read() throw (ActiveMQException);
		
		virtual int read( char* buffer, const int bufferSize ) throw (ActiveMQException);
		
		virtual void close() throw(cms::CMSException);
		
	private:
	
		void init( InputStream* stream, const int bufferSize );
		void bufferData() throw (ActiveMQException);
		
	private:
	
		InputStream* stream;
		char* buffer;
		int bufferSize;
		int head;
		int tail;
	};
	
}}

#endif /*ACTIVEMQ_IO_BUFFEREDINPUTSTREAM_H_*/
