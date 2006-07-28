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

#ifndef ACTIVEMQ_IO_BUFFEREDOUTPUTSTREAM_H_
#define ACTIVEMQ_IO_BUFFEREDOUTPUTSTREAM_H_
 
#include <activemq/io/OutputStream.h>

namespace activemq{
namespace io{
	
	class BufferedOutputStream : public OutputStream
	{
	public:
		BufferedOutputStream( OutputStream* stream );
		BufferedOutputStream( OutputStream* stream, const int bufSize );
		virtual ~BufferedOutputStream();
		
		virtual void write( const char c ) throw (ActiveMQException);
		
		virtual void write( const char* buffer, const int len ) throw (ActiveMQException);
		
		virtual void flush() throw (ActiveMQException);
		
		void close() throw(cms::CMSException);
		
	private:
	
		void init( OutputStream* stream, const int bufSize );
		
		/**
       	 * Writes the contents of the buffer to the output stream.
       	 */
      	void emptyBuffer() throw (ActiveMQException);
        
	private:
	
		OutputStream* stream;
		char* buffer;
		int bufferSize;
		int head;
		int tail;
	};

}}

#endif /*ACTIVEMQ_IO_BUFFEREDOUTPUTSTREAM_H_*/
