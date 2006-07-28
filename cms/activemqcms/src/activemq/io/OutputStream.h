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

#ifndef ACTIVEMQ_IO_OUTPUTSTREAM_H_
#define ACTIVEMQ_IO_OUTPUTSTREAM_H_
 
#include <cms/Closeable.h>
#include <activemq/ActiveMQException.h>

namespace activemq{
namespace io{

	class OutputStream : public cms::Closeable{
	public:
	
		virtual ~OutputStream(){}
		
		virtual void write( const char c ) throw (ActiveMQException) = 0;
		
		virtual void write( const char* buffer, const int len ) throw (ActiveMQException) = 0;
		
		virtual void flush() throw (ActiveMQException) = 0;
	};
		
}}

#endif /*ACTIVEMQ_IO_OUTPUTSTREAM_H_*/
