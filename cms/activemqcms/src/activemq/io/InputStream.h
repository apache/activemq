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

#ifndef ACTIVEMQ_IO_INPUTSTREAM_H_
#define ACTIVEMQ_IO_INPUTSTREAM_H_
 
#include <activemq/ActiveMQException.h>
#include <cms/Closeable.h>

namespace activemq{
namespace io{
	
	class InputStream : public cms::Closeable{
		
	public:
	
		virtual ~InputStream(){}
		
		virtual int available() const = 0;
		
		virtual char read() throw (ActiveMQException) = 0;
		
		virtual int read( char* buffer, const int bufferSize ) throw (ActiveMQException) = 0;
	};
	
}}

#endif /*ACTIVEMQ_IO_INPUTSTREAM_H_*/
