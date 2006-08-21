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

#ifndef ACTIVEMQ_IO_ENDIANWRITERTEST_H_
#define ACTIVEMQ_IO_ENDIANWRITERTEST_H_

#include <cppunit/TestFixture.h>
#include <cppunit/extensions/HelperMacros.h>

#include <activemq/util/Endian.h>
#include <activemq/io/EndianWriter.h>

namespace activemq{
namespace io{
	
	class EndianWriterTest : public CppUnit::TestFixture {
		
	  CPPUNIT_TEST_SUITE( EndianWriterTest );
	  CPPUNIT_TEST( test );
	  CPPUNIT_TEST_SUITE_END();
	  
	public:
	
		class MyOutputStream : public OutputStream{
		private:
			
			unsigned char buffer[1000];
			unsigned int pos;
		public:
		
			MyOutputStream(){
				pos = 0;
				memset( buffer, 0, 1000 );
			}
			virtual ~MyOutputStream(){}
			
			const unsigned char* getBuffer() const{ return buffer; }
			
			virtual void write( const unsigned char c ) throw (IOException){
				if( pos >= 1000 ){
					throw IOException();
				}
				
				buffer[pos++] = c;
			}
		
			virtual void write( const unsigned char* buffer, const int len ) throw (IOException){
				
				if( (pos + len) > 1000 ){
					throw IOException();
				}
				
				memcpy( this->buffer + pos, buffer, len );
				
				pos += len;
			}
		
			virtual void flush() throw (IOException){
			}

			virtual void close() throw(cms::CMSException){
				// do nothing.
			}
			
		    virtual void lock() throw(exceptions::ActiveMQException){
            }
            virtual void unlock() throw(exceptions::ActiveMQException){
            }
            virtual void wait() throw(exceptions::ActiveMQException){
            }
            virtual void wait(unsigned long millisecs) throw(exceptions::ActiveMQException){
            }
            virtual void notify() throw(exceptions::ActiveMQException){
            }
            virtual void notifyAll() throw(exceptions::ActiveMQException){
            }
		};
		
	public:
	
		virtual void setUp(){};	
	 	virtual void tearDown(){};
		void test(){
			
			unsigned char byteVal = (unsigned char)'T';
			uint16_t shortVal = 5;
			uint32_t intVal = 10000;
			uint64_t longVal = 1000000000;
			float floatVal = 10.0f;
			double doubleVal = 100.0;
			unsigned char arrayVal[3] = {
				'a', 'b', 'c'
			};
			
			// Create the stream with the buffer we just wrote to.
			MyOutputStream myStream;
			EndianWriter writer( &myStream );
			
			writer.writeByte( byteVal );
			writer.writeUInt16( shortVal );
			writer.writeUInt32( intVal );
			writer.writeUInt64( longVal );
			writer.writeFloat( floatVal );
			writer.writeDouble( doubleVal );
			writer.write( arrayVal, 3 );
			
			
			const unsigned char* buffer = myStream.getBuffer();
			int ix = 0;
			
			unsigned char tempByte = buffer[ix];
			CPPUNIT_ASSERT( tempByte == byteVal );
			ix += sizeof( tempByte );

			uint16_t tempShort = util::Endian::byteSwap( *(uint16_t*)(buffer+ix) );
			CPPUNIT_ASSERT( tempShort == shortVal );
			ix += sizeof( tempShort );
			
			uint32_t tempInt = util::Endian::byteSwap( *(uint32_t*)(buffer+ix) );
			CPPUNIT_ASSERT( tempInt == intVal );
			ix += sizeof( tempInt );
			
			uint64_t tempLong = util::Endian::byteSwap( *(uint64_t*)(buffer+ix) );
			CPPUNIT_ASSERT( tempLong == longVal );
			ix += sizeof( tempLong );
			
			float tempFloat = util::Endian::byteSwap( *(float*)(buffer+ix) );
			CPPUNIT_ASSERT( tempFloat == floatVal );
			ix += sizeof( tempFloat );
			
			double tempDouble = util::Endian::byteSwap( *(double*)(buffer+ix) );
			CPPUNIT_ASSERT( tempDouble == doubleVal );
			ix += sizeof( tempDouble );
		}

	};
	
}}

#endif /*ACTIVEMQ_IO_ENDIANWRITERTEST_H_*/
