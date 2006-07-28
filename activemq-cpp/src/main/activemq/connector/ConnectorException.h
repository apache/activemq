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
#ifndef CONNECTOREXCEPTION_H_
#define CONNECTOREXCEPTION_H_

#include <activemq/exceptions/ActiveMQException.h>

namespace activemq{
namespace connector{

    /*
     * Signals that an Connector exception of some sort has occurred.
     */
    class ConnectorException : public exceptions::ActiveMQException
    {
    public:
   
        ConnectorException() {}
        ConnectorException( const exceptions::ActiveMQException& ex ){
            *(ActiveMQException*)this = ex;
        }
        ConnectorException( const ConnectorException& ex ){
            *(exceptions::ActiveMQException*)this = ex;
        }
        ConnectorException( const char* file, 
                            const int lineNumber, 
                            const char* msg, ... )
        {
            va_list vargs;
            va_start( vargs, msg );
            buildMessage( msg, vargs );
            
            // Set the first mark for this exception.
            setMark( file, lineNumber );
        }
      
        /**
         * Clones this exception.  This is useful for cases where you need
         * to preserve the type of the original exception as well as the message.
         * All subclasses should override.
         */
        virtual exceptions::ActiveMQException* clone() const{
            return new ConnectorException( *this );
        }
        
   	    virtual ~ConnectorException() {}
   
    };

}}

#endif /*CONNECTOREXCEPTION_H_*/
