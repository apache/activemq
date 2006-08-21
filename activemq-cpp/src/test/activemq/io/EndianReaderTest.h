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

#ifndef ACTIVEMQ_IO_ENDIANREADERTEST_H_
#define ACTIVEMQ_IO_ENDIANREADERTEST_H_

#include <cppunit/TestFixture.h>
#include <cppunit/extensions/HelperMacros.h>

#include <activemq/exceptions/ActiveMQException.h>
#include <activemq/io/BufferedInputStream.h>
#include <activemq/util/Endian.h>
#include <activemq/io/EndianReader.h>

#ifdef min
#undef min
#endif

#include <algorithm>

namespace activemq{
namespace io{
	
	class EndianReaderTest : public CppUnit::TestFixture {
		
	  CPPUNIT_TEST_SUITE( EndianReaderTest );
	  CPPUNIT_TEST( test );
	  CPPUNIT_TEST_SUITE_END();
	  
	public:
	
		class MyInputStream : public InputStream{
		private:
			unsigned char* buffer;
			int len;
			unsigned int pos;
		public:
		
			MyInputStream( unsigned char* buffer, int len ){
				this->buffer = buffer;
				this->len = len;
				pos = 0;
			}
			virtual ~MyInputStream(){}
			
			virtual int available() const{
				return len - (int)pos;
			}
			virtual unsigned char read() throw (IOException){
				if( (int)pos >= len ){
					throw IOException();
				}
				
				return buffer[pos++];
			}
			virtual int read( unsigned char* buffer, const int bufferSize ) throw (IOException){
				unsigned int numToRead = std::min( bufferSize, available() );
				
				for( unsigned int ix=0; ix<numToRead; ++ix ){
					buffer[ix] = this->buffer[pos+ix];
				}
				
				pos += numToRead;
				
				return numToRead;
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
			
			unsigned char buffer[1000];			
			int ix = 0;	
			
			unsigned char byteVal = (unsigned char)'T';
			uint16_t shortVal = 5;
			uint32_t intVal = 10000;
			uint64_t longVal = 1000000000;
			float floatVal = 10.0f;
			double doubleVal = 100.0;
			unsigned char arrayVal[3] = {
				'a', 'b', 'c'
			};
			
			int size = sizeof(char);
			memcpy( (char*)(buffer+ix), (char*)&byteVal, size );
			ix += size;
			
			size = sizeof(uint16_t);
			uint16_t tempShort = util::Endian::byteSwap(shortVal);
			memcpy( (char*)(buffer+ix), (char*)&tempShort, size );
			ix += size;
			
			size = sizeof(uint32_t);
			uint32_t tempInt = util::Endian::byteSwap(intVal);
			memcpy( (char*)(buffer+ix), (char*)&tempInt, size );
			ix += size;
			
			size = sizeof(uint64_t);
			uint64_t tempLong = util::Endian::byteSwap(longVal);
			memcpy( (char*)(buffer+ix), (char*)&tempLong, size );
			ix += size;
			
			size = sizeof(float);
			float tempFloat = util::Endian::byteSwap(floatVal);
			memcpy( (char*)(buffer+ix), (char*)&tempFloat, size );
			ix += size;
			
			size = sizeof(double);
			double tempDouble = util::Endian::byteSwap(doubleVal);
			memcpy( (char*)(buffer+ix), (char*)&tempDouble, size );
			ix += size;
			
			size = 3;
			memcpy( (char*)(buffer+ix), (char*)&arrayVal, size );
			ix += size;

			// Create the stream with the buffer we just wrote to.
			MyInputStream myStream( buffer, 1000 );
			EndianReader reader( &myStream );
			
			byteVal = reader.readByte();
			CPPUNIT_ASSERT( byteVal == (unsigned char)'T' );
			
			shortVal = reader.readUInt16();
			CPPUNIT_ASSERT( shortVal == 5 );
			
			intVal = reader.readUInt32();
			CPPUNIT_ASSERT( intVal == 10000 );
			
			longVal = reader.readUInt64();
			CPPUNIT_ASSERT( longVal == 1000000000 );
			
			floatVal = reader.readFloat();
			CPPUNIT_ASSERT( floatVal == 10.0f );
			
			doubleVal = reader.readDouble();
			CPPUNIT_ASSERT( doubleVal == 100.0 );
			
			reader.read( arrayVal, 3 );
			CPPUNIT_ASSERT( arrayVal[0] == 'a' );
			CPPUNIT_ASSERT( arrayVal[1] == 'b' );
			CPPUNIT_ASSERT( arrayVal[2] == 'c' );
		}

	};
	
}}

#endif /*ACTIVEMQ_IO_ENDIANREADERTEST_H_*/
