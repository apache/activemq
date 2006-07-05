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
#ifndef ACTIVEMQ_IO_ENDIANREADER_H
#define ACTIVEMQ_IO_ENDIANREADER_H

#include <activemq/io/Reader.h>
#include <activemq/util/Endian.h>

namespace activemq{
namespace io{

	/*
	 * The BinaryReader class reads primitive C++ data types from an
	 * underlying input stream in a Java compatible way. Strings are
	 * read as raw bytes, no character decoding is performed.
	 *
	 * All numeric data types are assumed to be available in big
	 * endian (network byte order) and are converted automatically
	 * to little endian if needed by the platform.
	 *
	 * Should any error occur an IOException will be thrown.
	 */
	class EndianReader : public Reader
	{
	private:
	
		/**
		 * The target input stream.
		 */
		InputStream* inputStream;
		
	public:
	
		/**
		 * Constructor.
		 */
		EndianReader();
		
		/**
		 * Constructor.
		 * @param is the target input stream.
		 */
	    EndianReader( InputStream* is );
	    
	    /**
	     * Destructor.
	     */
	    virtual ~EndianReader();
	
		/**
		 * Sets the target input stream.
		 */
		virtual void setInputStream( InputStream* is ){
			inputStream = is;
		}
		
		/**
		 * Gets the target input stream.
		 */
	    virtual InputStream* getInputStream(){
	    	return inputStream;
	    }
		
		/**
	     * Attempts to read an array of bytes from the stream.
	     * @param buffer The target byte buffer.
	     * @param count The number of bytes to read.
	     * @return The number of bytes read.
	     * @throws IOException thrown if an error occurs.
	     */
	    virtual int read(unsigned char* buffer, int count) throw(IOException);
	    
	    /**
	     * Attempts to read a byte from the input stream
	     * @return The byte.
	     * @throws IOException thrown if an error occurs.
	     */
	    virtual unsigned char readByte() throw(IOException);
	    
	    /**
	     * Attempts to read a double from the input stream
	     * @return The byte.
	     * @throws IOException thrown if an error occurs.
	     */
	    virtual double readDouble() throw(IOException);
	    
	    /**
	     * Attempts to read a float from the input stream
	     * @return The byte.
	     * @throws IOException thrown if an error occurs.
	     */
	    virtual float readFloat() throw(IOException);
	    
	    /**
	     * Attempts to read a short from the input stream
	     * @return The byte.
	     * @throws IOException thrown if an error occurs.
	     */
	    virtual uint16_t readUInt16() throw(IOException);
	    
	    /**
	     * Attempts to read an int from the input stream
	     * @return The byte.
	     * @throws IOException thrown if an error occurs.
	     */
	    virtual uint32_t readUInt32() throw(IOException);
	    
	    /**
	     * Attempts to read a long long from the input stream
	     * @return The byte.
	     * @throws IOException thrown if an error occurs.
	     */
	    virtual uint64_t readUInt64() throw(IOException);
	};

}}

#endif /*ACTIVEMQ_IO_ENDIANREADER_H*/
