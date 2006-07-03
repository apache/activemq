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
#ifndef ACTIVEMQ_IO_READER_H
#define ACTIVEMQ_IO_READER_H

#include <string>
#include <activemq/io/IOException.h>
#include <activemq/io/InputStream.h>

namespace activemq{
namespace io{

	/*
	 * Reader interface that wraps around an input stream and provides
	 * an interface for extracting the data from the input stream.
	 */
	class Reader
	{
	public:
	
	    virtual ~Reader(){};
	
		/**
		 * Sets the target input stream.
		 */
		virtual void setInputStream( InputStream* is ) = 0;
		
		/**
		 * Gets the target input stream.
		 */
	    virtual InputStream* getInputStream() = 0;
	    
	    /**
	     * Attempts to read an array of bytes from the stream.
	     * @param buffer The target byte buffer.
	     * @param count The number of bytes to read.
	     * @return The number of bytes read.
	     * @throws IOException thrown if an error occurs.
	     */
	    virtual int read(unsigned char* buffer, int count) throw(IOException) = 0;
	    
	    /**
	     * Attempts to read a byte from the input stream
	     * @return The byte.
	     * @throws IOException thrown if an error occurs.
	     */
	    virtual unsigned char readByte() throw(IOException) = 0;
	} ;

}}

#endif /*ACTIVEMQ_IO_READER_H*/
