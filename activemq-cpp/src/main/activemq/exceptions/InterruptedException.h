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
#ifndef ACTIVEMQ_EXCEPTIONS_INTERRUPTEDENTEXCEPTION_H_
#define ACTIVEMQ_EXCEPTIONS_INTERRUPTEDENTEXCEPTION_H_

#include <activemq/exceptions/ActiveMQException.h>

namespace activemq{
namespace exceptions{

    /*
     * Thrown when an Thread is interrupted during a wait.
     */
    class InterruptedException : public ActiveMQException
    {
    public:

        /**
         * Default Constructor
         */
        InterruptedException(void) {};
        
        /**
         * Conversion Constructor from some other ActiveMQException
         * @param An exception that should become this type of Exception
         */
        InterruptedException(const ActiveMQException& ex){
            *(ActiveMQException*)this = ex;
        }

        /**
         * Copy Constructor
         */
        InterruptedException(const InterruptedException& ex){
            *(ActiveMQException*)this = ex;
        }

        /**
         * Constructor - Initializes the file name and line number where
         * this message occured.  Sets the message to report, using an 
         * optional list of arguments to parse into the message
         * @param file name where exception occurs
         * @param line number where the exception occurred.
         * @param message to report
         * @param list of primitives that are formatted into the message
         */
        InterruptedException( const char* file, 
                              const int lineNumber,
                              const char* msg, ... )
        {
            va_list vargs;
            va_start(vargs, msg);
            buildMessage(msg, vargs);
            
            // Set the first mark for this exception.
            setMark(file, lineNumber);
        }

        /**
         * Clones this exception.  This is useful for cases where you need
         * to preserve the type of the original exception as well as the message.
         * All subclasses should override.
         */
        virtual ActiveMQException* clone(void) const{
            return new InterruptedException(*this);
        }

        virtual ~InterruptedException(void) {}

   };

}}

#endif /*ACTIVEMQ_EXCEPTIONS_INTERRUPTEDENTEXCEPTION_H_*/
