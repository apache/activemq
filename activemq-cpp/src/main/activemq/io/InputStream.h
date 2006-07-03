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

#ifndef ACTIVEMQ_IO_INPUTSTREAM_H_
#define ACTIVEMQ_IO_INPUTSTREAM_H_
 
#include <activemq/io/IOException.h>
#include <cms/Closeable.h>
#include <activemq/concurrent/Synchronizable.h>

namespace activemq{
namespace io{
	
	/**
	 * Base interface for an input stream.
	 */
	class InputStream 
	: 
		public cms::Closeable,
		public concurrent::Synchronizable
	{
		
	public:
	
		virtual ~InputStream(){}
		
		/**
	     * Indcates the number of bytes avaialable.
	     * @return the number of bytes available on this input stream.
	     */
		virtual int available() const = 0;
		
		/**
		 * Reads a single byte from the buffer.
		 * @return The next byte.
		 * @throws IOException thrown if an error occurs.
		 */
		virtual unsigned char read() throw (IOException) = 0;
		
		/**
		 * Reads an array of bytes from the buffer.
		 * @param buffer (out) the target buffer.
		 * @param bufferSize the size of the output buffer.
		 * @return The number of bytes read.
		 * @throws IOException thrown if an error occurs.
		 */
		virtual int read( unsigned char* buffer, const int bufferSize ) throw (IOException) = 0;
	};
	
}}

#endif /*ACTIVEMQ_IO_INPUTSTREAM_H_*/
