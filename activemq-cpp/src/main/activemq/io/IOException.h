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
#ifndef ACTIVEMQ_IO_IOEXCEPTION_H
#define ACTIVEMQ_IO_IOEXCEPTION_H

#include <activemq/exceptions/ActiveMQException.h>

namespace activemq{
namespace io{

    /*
     * Signals that an I/O exception of some sort has occurred.
     */
    class IOException : public exceptions::ActiveMQException
    {
    public:

        IOException(){}
        IOException( const exceptions::ActiveMQException& ex ){
            *(exceptions::ActiveMQException*)this = ex;
        }
        IOException( const IOException& ex ){
            *(exceptions::ActiveMQException*)this = ex;
        }
        IOException( const char* file, const int lineNumber,
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
            return new IOException( *this );
        }
        
        virtual ~IOException(){}
        
    };

}}

#endif /*ACTIVEMQ_IO_IOEXCEPTION_H*/
