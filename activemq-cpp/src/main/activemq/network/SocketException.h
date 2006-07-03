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
#ifndef ACTIVEMQ_NETWORK_SOCKETEXCEPTION_H
#define ACTIVEMQ_NETWORK_SOCKETEXCEPTION_H

#include <activemq/io/IOException.h>

namespace activemq{
namespace network{

	/**
	 * Exception for errors when manipulating sockets.
	 */
	class SocketException : public io::IOException
	{
	public:
	
		SocketException(){}
        SocketException( const ActiveMQException& ex ){
            *(ActiveMQException*)this = ex;
        }
        SocketException( const SocketException& ex ){
            *(ActiveMQException*)this = ex;
        }
	    SocketException(const char* file, const int lineNumber, 
            const char* msg, ...)
	    {
	        va_list vargs ;
            va_start(vargs, msg) ;
            buildMessage(msg, vargs) ;
            
            // Set the first mark for this exception.
            setMark( file, lineNumber );
	    }
        
        /**
         * Clones this exception.  This is useful for cases where you need
         * to preserve the type of the original exception as well as the message.
         * All subclasses should override.
         */
        virtual ActiveMQException* clone() const{
            return new SocketException( *this );
        }
        
	    virtual ~SocketException(){}
	} ;

}}


#endif // ACTIVEMQ_NETWORK_SOCKETEXCEPTION_H

