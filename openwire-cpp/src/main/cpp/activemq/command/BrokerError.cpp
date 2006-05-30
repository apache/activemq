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
#include "activemq/command/BrokerError.hpp"

using namespace apache::activemq::command;

/*
 * 
 */
BrokerError::BrokerError()
{
    message            = new string() ;
    exceptionClass     = new string() ;
    stackTraceElements = NULL ;
    cause              = NULL ;
}

/*
 * 
 */
p<string> BrokerError::getMessage()
{
    return message ;
}

/*
 * 
 */
void BrokerError::setMessage(const char* msg)
{
    this->message->assign(msg) ;
}

/*
 * 
 */
p<string> BrokerError::getExceptionClass()
{
    return exceptionClass ;
}

/*
 * 
 */
void BrokerError::setExceptionClass(const char* exClass)
{
    this->exceptionClass->assign(exClass) ;
}

/*
 * 
 */
array<StackTraceElement> BrokerError::getStackTraceElements()
{
    return stackTraceElements ;
}

/*
 * 
 */
void BrokerError::setStackTraceElements(array<StackTraceElement> elements)
{
    this->stackTraceElements = elements ;
}

/*
 * 
 */
p<BrokerError> BrokerError::getCause()
{
    return cause ;
}

/*
 * 
 */
void BrokerError::setCause(p<BrokerError> cause)
{
    this->cause = cause ;
}

/*
 * 
 */
p<string> BrokerError::getStackTrace()
{
    ostringstream sstream ;
    p<string> trace ;
    printStackTrace(sstream) ;
    trace = new string (sstream.str());
    return trace ;
}

/*
 * 
 */
void BrokerError::printStackTrace(ostream& out)
{
    out << getptr(exceptionClass) << ": " << getptr(message) << endl ;

    for( size_t i = 0; i < sizeof(stackTraceElements)/sizeof(stackTraceElements[0]); i++ )
    {
        p<StackTraceElement> element = stackTraceElements[i] ;
        out << "    at " << getptr(element->className) << "." << getptr(element->methodName) << "(" << getptr(element->fileName) << ":" << element->lineNumber << ")" << endl ;
    }
    // Dump any nested exceptions
    if( cause != NULL )
    {
        out << "Nested Exception:" << endl ;
        cause->printStackTrace(out) ;
    }
}
