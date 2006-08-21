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

#ifndef ACTIVEMQ_IO_BYTEARRAYOUTPUTSTREAMTEST_H_
#define ACTIVEMQ_IO_BYTEARRAYOUTPUTSTREAMTEST_H_

#include <cppunit/TestFixture.h>
#include <cppunit/extensions/HelperMacros.h>

#include <activemq/io/ByteArrayOutputStream.h>

namespace activemq{
namespace io{

   class ByteArrayOutputStreamTest : public CppUnit::TestFixture 
   {
     CPPUNIT_TEST_SUITE( ByteArrayOutputStreamTest );
     CPPUNIT_TEST( testStream );
     CPPUNIT_TEST_SUITE_END();

   public:

   	ByteArrayOutputStreamTest() {}

   	virtual ~ByteArrayOutputStreamTest() {}

      void testStream()
      {
         ByteArrayOutputStream stream_a;
         
         stream_a.write('a');
         stream_a.write(60);
         stream_a.write('c');
         
         CPPUNIT_ASSERT( stream_a.getByteArraySize() == 3 );
         
         stream_a.clear();
         
         CPPUNIT_ASSERT( stream_a.getByteArraySize() == 0 );
         
         stream_a.write((const unsigned char*)("abc"), 3);
  
         CPPUNIT_ASSERT( stream_a.getByteArraySize() == 3 );
         
         stream_a.clear();

         CPPUNIT_ASSERT( stream_a.getByteArraySize() == 0 );
         
         stream_a.write((const unsigned char*)("abc"), 3);

         unsigned char buffer[4];
         
         memset(buffer, 0, 4);
         memcpy(buffer, stream_a.getByteArray(), stream_a.getByteArraySize());
         
         CPPUNIT_ASSERT( std::string((const char*)buffer) == std::string("abc") );
      }
   };

}}

#endif /*ACTIVEMQ_IO_BYTEARRAYOUTPUTSTREAMTEST_H_*/
